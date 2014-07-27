#include <gtest/gtest.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TFileTransport.h>
#include <boost/thread/thread.hpp>

#include "runtime/data-stream-mgr.h"
#include "runtime/data-stream-sender.h"
#include "runtime/descriptors.h"
#include "runtime/mem-tracker.h"
#include "codegen/llvm-codegen.h"
#include "runtime/plan-fragment-executor.h"
#include "runtime/row-batch.h"
#include "util/disk-info.h"
#include "util/mem-info.h"
#include "util/thread.h"
#include "util/debug-util.h"
#include "runtime/mem-tracker.h"
#include "gen-cpp/ImpalaInternalService_types.h"
#include "gen-cpp/ImpalaInternalService.h"
#include "gen-cpp/Partitions_types.h"

#define REQUEST(request, file) \
  impala::TExecPlanFragmentParams request; \
  load_request(file, request);

#define BATCH(obj, file) \
  impala::TRowBatch obj; \
  load_request(file, obj);

#define REQUEST_SELECT_1 REQUEST(request, "query_select_1002.bin");
#define REQUEST_SELECT_FROM_TBL REQUEST(request, "query_select_from_tbl.bin");

boost::scoped_ptr<impala::ExecEnv> env;
impala::ObjectPool obj_pool_;

template <class T>
void load_request(const char* filename, T& req)
{
  using namespace apache::thrift::protocol;
  using namespace apache::thrift::transport;

  boost::shared_ptr<TFileTransport> transport(new TFileTransport(filename, true));
  boost::shared_ptr<TBinaryProtocol> proto(new TBinaryProtocol(transport));
  req.read(proto.get());
}


TEST(ExecutorTest, Basic)
{
    impala::PlanFragmentExecutor* ptr;
    ptr = new impala::PlanFragmentExecutor(env.get(),
                                           NULL);
    REQUEST_SELECT_1
    ASSERT_EQ(ptr->Prepare(request).code(), impala::TStatusCode::OK);
    delete ptr;
}

TEST(ExecutorTest, JustOpen)
{
  using namespace impala;
  PlanFragmentExecutor exec(env.get(), NULL);

  REQUEST_SELECT_1

  ASSERT_EQ(exec.Prepare(request).code(), TStatusCode::OK);
  ASSERT_EQ(exec.Open().code(), TStatusCode::OK);
}

TEST(ExecutorTest, GetResult)
{
  using namespace impala;
  PlanFragmentExecutor exec(env.get(), NULL);

  REQUEST_SELECT_1

  ASSERT_EQ(exec.Prepare(request).code(), TStatusCode::OK);
  ASSERT_EQ(exec.Open().code(), TStatusCode::OK);

  RowBatch* batch = NULL;
  ASSERT_EQ(exec.GetNext(&batch).code(), TStatusCode::OK);

  ASSERT_EQ(batch->num_rows(), 1) << "Result should have 1 row.";
  ASSERT_STREQ(PrintRow(batch->GetRow(0), batch->row_desc()).c_str(), "[(1002)]");
}

TEST(ExecutorTest, SelectFromTable)
{
  using namespace impala;
  PlanFragmentExecutor exec(env.get(), NULL);

  REQUEST_SELECT_FROM_TBL
  BATCH(obj, "TRowBatch.bin");

  ASSERT_NE(ExecEnv::GetInstance(), (ExecEnv*)NULL);
  ASSERT_EQ(exec.Prepare(request).code(), TStatusCode::OK);
  env->stream_mgr()->AddData(request.fragment_instance_ctx.fragment_instance_id, 1, obj, 0);

  RowBatch* batch = NULL;

  ASSERT_EQ(exec.Open().code(), TStatusCode::OK);
  env->stream_mgr()->CloseSender(request.fragment_instance_ctx.fragment_instance_id, 1, 0);
  ASSERT_EQ(exec.GetNext(&batch).code(), TStatusCode::OK);
  ASSERT_EQ(batch->num_rows(), 5);
  ASSERT_STREQ(PrintRow(batch->GetRow(0), batch->row_desc()).c_str(), "[(1 1)]");
  ASSERT_STREQ(PrintRow(batch->GetRow(1), batch->row_desc()).c_str(), "[(2 2)]");
  ASSERT_STREQ(PrintRow(batch->GetRow(2), batch->row_desc()).c_str(), "[(3 3)]");
  ASSERT_STREQ(PrintRow(batch->GetRow(3), batch->row_desc()).c_str(), "[(4 4)]");
  ASSERT_STREQ(PrintRow(batch->GetRow(4), batch->row_desc()).c_str(), "[(10 10)]");
}

TEST(ExecutorTest, ScanNode)
{
  using namespace impala;
  PlanFragmentExecutor exec(env.get(), NULL);

  // this serialized query contains HdfsScanNode part
  REQUEST(request, "select_*_from_test.text_ass.bin.2");
  request.fragment.__isset.output_sink = false;
//  request.desc_tbl.tableDescriptors[0].hdfsTable.hdfsBaseDir = "";
//  request.desc_tbl.tableDescriptors[0].hdfsTable.partitions[2].file_desc[0].file_name = "table_text.0";
  request.params.per_node_scan_ranges[0][0].scan_range.hdfs_file_split.path = "table_text.0";

  ASSERT_EQ(exec.Prepare(request).code(), TStatusCode::OK);
  ASSERT_EQ(exec.Open().code(), TStatusCode::OK);

  RowBatch* batch = NULL;
  ASSERT_EQ(exec.GetNext(&batch).code(), TStatusCode::OK);
  ASSERT_NE(batch, (RowBatch*)NULL);
  ASSERT_EQ(batch->num_rows(), 5);
  ASSERT_STREQ(PrintRow(batch->GetRow(0), batch->row_desc()).c_str(), "[(1 1)]");
  ASSERT_STREQ(PrintRow(batch->GetRow(1), batch->row_desc()).c_str(), "[(2 2)]");
  ASSERT_STREQ(PrintRow(batch->GetRow(2), batch->row_desc()).c_str(), "[(3 3)]");
  ASSERT_STREQ(PrintRow(batch->GetRow(3), batch->row_desc()).c_str(), "[(4 4)]");
  ASSERT_STREQ(PrintRow(batch->GetRow(4), batch->row_desc()).c_str(), "[(10 10)]");
}

TEST(ExecutorTest, ScanNodeWithParquetFile)
{
  using namespace impala;
  PlanFragmentExecutor exec(env.get(), NULL);

  // this serialized query contains HdfsScanNode part
  REQUEST(request, "select_*_from_test.parqu.bin.2");
  request.fragment.__isset.output_sink = false;
//  request.desc_tbl.tableDescriptors[0].hdfsTable.hdfsBaseDir = "";
//  request.desc_tbl.tableDescriptors[0].hdfsTable.partitions[2].file_desc[0].file_name = "table_text.0";
  request.params.per_node_scan_ranges[0][0].scan_range.hdfs_file_split.path = "table_parquet.0";

  ASSERT_EQ(exec.Prepare(request).code(), TStatusCode::OK);
  ASSERT_EQ(exec.Open().code(), TStatusCode::OK);

  RowBatch* batch = NULL;
  ASSERT_EQ(exec.GetNext(&batch).code(), TStatusCode::OK);
  ASSERT_NE(batch, (RowBatch*)NULL);
  ASSERT_EQ(batch->num_rows(), 3);
  ASSERT_STREQ(PrintRow(batch->GetRow(0), batch->row_desc()).c_str(), "[(1)]");
  ASSERT_STREQ(PrintRow(batch->GetRow(1), batch->row_desc()).c_str(), "[(2)]");
  ASSERT_STREQ(PrintRow(batch->GetRow(2), batch->row_desc()).c_str(), "[(3)]");
}

TEST(ExecutorTest, TwoFragmentsConnectedManually)
{
  using namespace impala;
  PlanFragmentExecutor exec(env.get(), NULL);

  // ScanNode
  REQUEST(request, "select_*_from_test.text_ass_order_by_value_desc_limit_2.bin.5");
  request.fragment.__isset.output_sink = false;
  request.params.per_node_scan_ranges[0][0].scan_range.hdfs_file_split.path = "table_text.0";
  const int32_t dest_node_id = request.fragment.output_sink.stream_sink.dest_node_id;

  ASSERT_EQ(exec.Prepare(request).code(), TStatusCode::OK);
  ASSERT_EQ(exec.Open().code(), TStatusCode::OK);

  RowBatch* batch = NULL;
  ASSERT_EQ(exec.GetNext(&batch).code(), TStatusCode::OK);
  ASSERT_NE(batch, (RowBatch*)NULL);
  ASSERT_EQ(batch->num_rows(), 2);
  ASSERT_STREQ(PrintRow(batch->GetRow(0), batch->row_desc()).c_str(), "[(10 10)]");
  ASSERT_STREQ(PrintRow(batch->GetRow(1), batch->row_desc()).c_str(), "[(4 4)]");
  TRowBatch output_batch;
  batch->Serialize(&output_batch);
  exec.Close();

  // Exchange node
  REQUEST(request2, "select_*_from_test.text_ass_order_by_value_desc_limit_2.bin.4");

  ASSERT_EQ(exec.Prepare(request2).code(), TStatusCode::OK);
  env->stream_mgr()->AddData(request2.fragment_instance_ctx.fragment_instance_id, dest_node_id, output_batch, 0);

  ASSERT_EQ(exec.Open().code(), TStatusCode::OK);
  env->stream_mgr()->CloseSender(request2.fragment_instance_ctx.fragment_instance_id, dest_node_id, 0);
  ASSERT_EQ(exec.GetNext(&batch).code(), TStatusCode::OK);
  ASSERT_EQ(batch->num_rows(), 2);
  ASSERT_STREQ(PrintRow(batch->GetRow(0), batch->row_desc()).c_str(), "[(10 10)]");
  ASSERT_STREQ(PrintRow(batch->GetRow(1), batch->row_desc()).c_str(), "[(4 4)]");
  exec.Close();
}
TEST(ExecutorTest, TwoFragmentsConnectedAutomatically)
{
  FAIL();
}


int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  impala::CpuInfo::Init();
  impala::DiskInfo::Init();
  impala::MemInfo::Init();
  impala::InitThreading();
  impala::TimestampParser::Init();
  impala::LlvmCodeGen::InitializeLlvm();
  env.reset(new impala::ExecEnv());
  // hack
  env->InitForFeTests();
  ::testing::InitGoogleTest(&argc, argv);
  int r = RUN_ALL_TESTS();
  env.reset();
  return r;
}


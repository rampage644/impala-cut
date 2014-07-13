#include <gtest/gtest.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TFileTransport.h>

#include "runtime/data-stream-mgr.h"
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

#define REQUEST(file) \
  impala::TExecPlanFragmentParams request; \
  load_request(file, request);

#define REQUEST_SELECT_1 REQUEST("query_select_1002.bin");
#define REQUEST_SELECT_FROM_TBL REQUEST("query_select_from_tbl.bin");

boost::scoped_ptr<impala::ExecEnv> env;

void load_request(const char* filename, impala::TExecPlanFragmentParams& req)
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

  ASSERT_NE(ExecEnv::GetInstance(), (ExecEnv*)NULL);
  ASSERT_EQ(exec.Prepare(request).code(), TStatusCode::OK);
  ASSERT_EQ(exec.Open().code(), TStatusCode::OK);

  RowBatch* batch = NULL;
  env->stream_mgr()-
  ASSERT_EQ(exec.GetNext(&batch).code(), TStatusCode::OK);

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
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}


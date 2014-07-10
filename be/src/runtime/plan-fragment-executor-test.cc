#include <gtest/gtest.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TFileTransport.h>

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

void load_request(impala::TExecPlanFragmentParams& req)
{
  using namespace apache::thrift::protocol;
  using namespace apache::thrift::transport;

  boost::shared_ptr<TFileTransport> transport(new TFileTransport("TExecPlanFragmentParams.bin", true));
  boost::shared_ptr<TBinaryProtocol> proto(new TBinaryProtocol(transport));
  req.read(proto.get());
}

TEST(PlanFragmentExecutorTest, Basic)
{
    impala::ExecEnv* env = new impala::ExecEnv();
    impala::PlanFragmentExecutor* ptr;
    ptr = new impala::PlanFragmentExecutor(env,
                                           NULL);
    impala::TExecPlanFragmentParams params;
    load_request(params);
    ASSERT_EQ(ptr->Prepare(params).code(), impala::TStatusCode::OK);
    delete ptr;
    delete env;
}

TEST(PlanFragmentExecutorTest, JustOpen)
{
  using namespace impala;
  ExecEnv env;
  PlanFragmentExecutor exec(&env, NULL);

  TExecPlanFragmentParams params;
  load_request(params);

  ASSERT_EQ(exec.Prepare(params).code(), TStatusCode::OK);
  ASSERT_EQ(exec.Open().code(), TStatusCode::OK);
}

TEST(PlanFragmentExecutorTest, GetResult)
{
  using namespace impala;
  ExecEnv env;
  PlanFragmentExecutor exec(&env, NULL);

  TExecPlanFragmentParams params;
  load_request(params);

  ASSERT_EQ(exec.Prepare(params).code(), TStatusCode::OK);
  ASSERT_EQ(exec.Open().code(), TStatusCode::OK);

  RowBatch* batch = NULL;
  ASSERT_EQ(exec.GetNext(&batch).code(), TStatusCode::OK);

  ASSERT_EQ(batch->num_rows(), 1) << "Result should have 1 row.";
  ASSERT_STREQ(PrintRow(batch->GetRow(0), batch->row_desc()).c_str(), "[(1002)]");
}


int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  impala::CpuInfo::Init();
  impala::DiskInfo::Init();
  impala::MemInfo::Init();
  impala::InitThreading();
  impala::TimestampParser::Init();
  impala::LlvmCodeGen::InitializeLlvm();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}


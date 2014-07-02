#include <memory>

#include <gtest/gtest.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TSimpleFileTransport.h>

#include "codegen/llvm-codegen.h"
#include "runtime/plan-fragment-executor.h"
#include "util/disk-info.h"
#include "util/thread.h"
#include "gen-cpp/ImpalaInternalService_types.h"
#include "gen-cpp/ImpalaInternalService.h"

TEST(PlanFragmentExecutorTest, Basic)
{
    impala::PlanFragmentExecutor* ptr;
    ptr = new impala::PlanFragmentExecutor(NULL,
                                           NULL);
    impala::TExecPlanFragmentParams params;
    ptr->Prepare(params);
    delete ptr;
}


int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  impala::CpuInfo::Init();
  impala::DiskInfo::Init();
  impala::InitThreading();
  impala::TimestampParser::Init();
  impala::LlvmCodeGen::InitializeLlvm();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

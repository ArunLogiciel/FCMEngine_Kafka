#include "../FeeModule/FeePlanExecutor.h"
#include "../FeeModule/Utility.h"
#include "../FeeModule/logger/logger.hpp"

LSL::FeeModule::FeePlanExecutor::FeePlanExecutor(Interfaces::MethodInvokerInterface& methodInvoker, Interfaces::PlanExecutorStoreInterface& store) :
	m_methodInvoker(methodInvoker),
	m_store(store)
{
}


void LSL::FeeModule::FeePlanExecutor::init()
{
}

void LSL::FeeModule::FeePlanExecutor::finish()
{
}

FeeBreakDown LSL::FeeModule::FeePlanExecutor::Execute(Plan plans, OrderExecutionData& data)
{
	//LogDebug() << __PRETTY_FUNCTION__ << " 'called'";
	DEBUG_LOG("FeeModule", fmt::format("{} - called", __PRETTY_FUNCTION__));
	std::string key = GetKey(plans, data);
	const Fee oldOrderFee = m_store.GetExecutorState(plans.planType, key);

	FeeBreakDown feeInfo;
	
	Fee val = m_methodInvoker.Invoke(plans.id, data);
	val = Utility::RoundOff(val, plans.roundingPoints, plans.rounding);

	feeInfo.orderFee = oldOrderFee + val;
	feeInfo.executionFee = val;

	m_store.SetExecutorState(plans.planType, key, feeInfo.orderFee);

	return feeInfo;
}

std::string LSL::FeeModule::FeePlanExecutor::GetKey(const Plan& plans, const OrderExecutionData& data)
{
	return plans.id + "_" + data.orderId;
}


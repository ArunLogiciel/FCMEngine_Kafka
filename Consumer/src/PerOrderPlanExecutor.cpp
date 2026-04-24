#include "../FeeModule/PerOrderPlanExecutor.h"
#include "../FeeModule/Utility.h"
#include "../FeeModule/logger/logger.hpp"

LSL::FeeModule::PerOrderPlanExecutor::PerOrderPlanExecutor(Interfaces::MethodInvokerInterface& methodInvoker, Interfaces::PlanExecutorStoreInterface& store) :
	m_methodInvoker(methodInvoker),
	m_store(store)
{
}

void LSL::FeeModule::PerOrderPlanExecutor::init()
{
}

void LSL::FeeModule::PerOrderPlanExecutor::finish()
{
}

FeeBreakDown LSL::FeeModule::PerOrderPlanExecutor::Execute(Plan plans, OrderExecutionData& data)
{
	//LogDebug() << __PRETTY_FUNCTION__ << " 'called'";
	DEBUG_LOG("FeeModule", fmt::format("{} - called", __PRETTY_FUNCTION__));
	std::string key = GetKey(plans, data);
	const Fee oldOrderFee = m_store.GetExecutorState(plans.planType, key);

	FeeBreakDown feeInfo;
	
	//Using Avg Px and Total Executed Quanity of Order [SHOULD ONLY BE APPLIED ON LAST FILL]
	OrderExecutionData data2 = data;
	data2.price = data.avgPx;
	data2.quantity = data.execQty;

	Fee val = m_methodInvoker.Invoke(plans.id, data2);

	val = Utility::RoundOff(val,plans.roundingPoints,plans.rounding);

	feeInfo.orderFee = val;
	feeInfo.executionFee = val - oldOrderFee;

	m_store.SetExecutorState(plans.planType, key, feeInfo.orderFee);

	return feeInfo;
}

std::string LSL::FeeModule::PerOrderPlanExecutor::GetKey(const Plan& plans, const OrderExecutionData& data)
{
	return plans.id + "_" + data.orderId;
}



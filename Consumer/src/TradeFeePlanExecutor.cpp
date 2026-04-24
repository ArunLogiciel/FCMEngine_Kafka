#include "../FeeModule/pch.h"
#include "../FeeModule/TradeFeePlanExecutor.h"
#include "../FeeModule/PerExecutionPlanExecutor.h"
#include "../FeeModule/PerOrderPlanExecutor.h"
#include "../FeeModule/PerSymbolPlanExecutor.h"	//Not Yet Implemented
#include "../FeeModule/FeePlanExecutor.h"
#include "../FeeModule/DbDataAdapter.h"
#include "../FeeModule/logger/logger.hpp"


LSL::FeeModule::TradeFeePlanExecutor::TradeFeePlanExecutor(Interfaces::MethodInvokerInterface& methodInvoker, Interfaces::PlanExecutorStoreInterface& store) :
m_methodInvoker(methodInvoker),
m_store(store)
{
}

void LSL::FeeModule::TradeFeePlanExecutor::init()
{
	//LogDebug() << __PRETTY_FUNCTION__ << " 'Initializing'";
	DEBUG_LOG("FeeModule", fmt::format("{} - Initializing", __PRETTY_FUNCTION__));
	m_planTypeExecutors.emplace(std::make_pair(EN_PlanType_PerExecution, new PerExecutionPlanExecutor(m_methodInvoker, m_store)));
	m_planTypeExecutors.emplace(std::make_pair(EN_PlanType_PerOrder, new PerOrderPlanExecutor(m_methodInvoker, m_store)));
	//m_planTypeExecutors.emplace(std::make_pair(EN_PlanType_FeeRule, new FeePlanExecutor(m_methodInvoker, m_store)));

	//LogDebug() << __PRETTY_FUNCTION__ << " 'Initializing PlanTypeExecutors'";
	DEBUG_LOG("FeeModule", fmt::format("{} - Initializing PlanTypeExecutors", __PRETTY_FUNCTION__));
	for (MAP_PLAN_TYPE_EXECUTOR::iterator it = m_planTypeExecutors.begin(); it != m_planTypeExecutors.end(); ++it)
	{
		it->second->init();
	}

	std::vector<FeeType> feeType;
	DbDataAdapter::getInstance()->dataRetrievedTradeFeeCategories(feeType);
	//LogDebug() << __PRETTY_FUNCTION__ << " 'Inserting Retrieved FeeTypes from DB to MAPs'";
	DEBUG_LOG("FeeModule", fmt::format("{} - Inserting Retrieved FeeTypes from DB to MAPs", __PRETTY_FUNCTION__));
	for (std::vector<FeeType>::iterator it = feeType.begin(); it != feeType.end(); ++it)
	{
		m_feeTypes.insert(std::make_pair(it->id, *it));
	}
	//LogInfo() << __PRETTY_FUNCTION__ << " 'Initialized'";
	INFO_LOG("FeeModule", fmt::format("{} - Initialized", __PRETTY_FUNCTION__));
}

void LSL::FeeModule::TradeFeePlanExecutor::finish()
{
	for (MAP_PLAN_TYPE_EXECUTOR::iterator it = m_planTypeExecutors.begin(); it != m_planTypeExecutors.end(); ++it)
	{
		it->second->finish();
		delete it->second;
	}
	m_planTypeExecutors.clear();
	m_feeTypes.clear();
	//LogInfo() << __PRETTY_FUNCTION__ << " 'Finished Successfully'";
	INFO_LOG("FeeModule", fmt::format("{} - Finished Successfully", __PRETTY_FUNCTION__));
}

FeeComissionData LSL::FeeModule::TradeFeePlanExecutor::Execute(TradeFees &tradeFees, PlansMap &m_plans, OrderExecutionData& data)
{
	//LogDebug() << __PRETTY_FUNCTION__ << " 'called for OrderID:" << data.orderId << "| ExecutionID :" << data.executionId << "'";
	DEBUG_LOG("FeeModule", fmt::format("{} - called for OrderID: {} | ExecutionID :{}", __PRETTY_FUNCTION__, data.orderId, data.executionId));

	FeeComissionData feeComissionData;
	for (TradeFees::iterator it = tradeFees.begin(); it != tradeFees.end(); ++it)
	{
		std::string planId = it->second.planId;
		PlansMap::iterator itPlan = m_plans.find(planId);
		if (itPlan == m_plans.end())
			continue;
		
		std::string feeTypeId = itPlan->second.feeCategoryId; 
		MAP_FEE_TYPE::iterator itfeeType = m_feeTypes.find(feeTypeId);
		if (itfeeType == m_feeTypes.end())
			continue;

		int planType = (int)itPlan->second.planType;
		MAP_PLAN_TYPE_EXECUTOR::iterator itPlanExecutor = m_planTypeExecutors.find((PlanType)planType);
		if (itPlanExecutor == m_planTypeExecutors.end())
			continue;

		//LogDebug() << __PRETTY_FUNCTION__ << " 'Executing For PlanId: " << planId<< " ' | Executing For FeeCategoryId: " << feeTypeId<< " 'Executor Type: " << static_cast<int>(itPlanExecutor->first) << "'";
		DEBUG_LOG("FeeModule", fmt::format("{} - Executing For PlanId: {} | Executing For FeeCategoryId:{} |Executor Type:{}", __PRETTY_FUNCTION__, planId, feeTypeId, static_cast<int>(itPlanExecutor->first)));
		FeeBreakDown fee = itPlanExecutor->second->Execute(itPlan->second, data);
				
		if (isCommission(itfeeType->second))
		{
			feeComissionData.comission.executionFee = fee.executionFee;
			feeComissionData.comission.orderFee = fee.orderFee;
		}
			

		if (isTradingFee(itfeeType->second))
		{
			feeComissionData.totalTradeFee.executionFee += fee.executionFee;
			feeComissionData.totalTradeFee.orderFee += fee.orderFee;
		}
		//LogDebug() << __PRETTY_FUNCTION__ << " 'Execution Fee Calculated'";
		DEBUG_LOG("FeeModule", fmt::format("{} Execution Fee Calculated", __PRETTY_FUNCTION__));
		fee.feeCategoryId = itfeeType->second.id;
		fee.planId = itPlan->second.id;

		feeComissionData.tradeFeeBreakDown.emplace_back(fee);
	}
	return feeComissionData;
}

bool LSL::FeeModule::TradeFeePlanExecutor::isCommission(FeeType feeType)
{
	return feeType.name == "Commission";
}

bool LSL::FeeModule::TradeFeePlanExecutor::isTradingFee(FeeType feeType)
{
	return !isCommission(feeType);
}

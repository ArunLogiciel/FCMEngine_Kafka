//#include "pch.h"
#include "../FeeModule/FeeComissionManager.h"
#include "../FeeModule/DbDataAdapter.h"
#include "boost/config/detail/suffix.hpp"
#include <boost/algorithm/string/case_conv.hpp>

//#include "StackLogger.h" // For DBlibrary Logging

LSL::FeeModule::FeeComissionManager::FeeComissionManager(IniParser& settings, const std::string& date)
	: runDate(date)
	, m_configSettings(std::move(settings))
	, m_methodInvoker(std::make_shared<MethodInvoker>())
	, m_store(std::make_shared<PlanExecutorStore>(m_configSettings.Get<int>("FCMEngine.isEnabled")))
	, m_planExecutor(std::make_unique <TradeFeePlanExecutor>(*m_methodInvoker, *m_store))
	, m_planSelector(std::make_unique <PlanSelector>())
	, m_FCStore(std::make_unique <FeeCommissionStore>(date, m_configSettings.Get<int>("FCMEngine.isEnabled")))
	, m_cliManager(*m_methodInvoker, m_configSettings.Get<int>("CLI.Port"), m_configSettings.Get<int>("FCMEngine.isEnabled"))
	, m_symbolStore(m_configSettings, runDate)
	, m_holidayStore(m_configSettings)
{
	//LSL::DBL::Logger::getInstance(m_configSettings.Get<std::string>("Logging.Path"));
	DbDataAdapter::getInstance(std::move(m_configSettings), runDate);
}

void LSL::FeeModule::FeeComissionManager::init()
{
	//LogInfo() << __PRETTY_FUNCTION__ << " 'Initializing'";
	//INFO_LOG("FeeModule", fmt::format("{} - Initializing ", __PRETTY_FUNCTION__));
	// initialize all the components
	try
	{
		m_accountStore.init();
		m_symbolStore.init();
		m_holidayStore.init();  //This is the first step to initialize Holiday Store as we need to populate holidays set before anything else, as it is being used in multiple places for different purposes like plan selection, plan execution and also in CLI for validations
		m_FCStore->init(m_accountStore);
		m_planSelector->init(m_accountStore.m_accounts);
		m_planExecutor->init();
		m_store->init();
		m_methodInvoker->init();
		m_cliManager.init();
		m_plans = m_planSelector->GetPlans(); // Where To Keep This?	
		DbDataAdapter::getInstance()->disconnectStaticDb();
	}
	catch (std::exception& ex)
	{
		//LogWarning() << __PRETTY_FUNCTION__ << " 'Failed to Initialize FeeCommissionManager : " << ex.what() << "'";
		//WARNING_LOG("FeeModule", fmt::format("{} - 'Failed to Initialize FeeCommissionManager:{}", __PRETTY_FUNCTION__, ex.what()));
		std::abort(); //Should we ABORT incase Initialization Fails?
		//throw std::exception("Failed to Initialize FeeCommissionManager");
	}
	//LogInfo() << __PRETTY_FUNCTION__ << " 'Initialization Completed'";
	//INFO_LOG("FeeModule", fmt::format("{} - Initialization Completed ", __PRETTY_FUNCTION__));
}

//For Back Dated Adjustments
void LSL::FeeModule::FeeComissionManager::init(Adjustment& adjustment)
{
	//LogInfo() << __PRETTY_FUNCTION__ << " 'Initializing For Back Date Adjustment'";
	//INFO_LOG("FeeModule", fmt::format("{} - Initializing For Back Date Adjustment ", __PRETTY_FUNCTION__));
	try
	{
		m_accountStore.init(adjustment);
		m_symbolStore.init();
		m_FCStore->init(adjustment);
		m_planSelector->init(adjustment);
		m_planExecutor->init();
		m_store->init();
		m_methodInvoker->init(adjustment);
		m_plans = m_planSelector->GetPlans();
	}
	catch (std::exception& ex)
	{
		//LogWarning() << __PRETTY_FUNCTION__ << "Failed to Initialize FeeCommissionManager : " << ex.what();
		//WARNING_LOG("FeeModule", fmt::format("{} - 'Failed to Initialize FeeCommissionManager:{}", __PRETTY_FUNCTION__, ex.what()));
		throw std::exception("Failed to Initialize FeeCommissionManager");
	}

	//LogInfo() << __PRETTY_FUNCTION__ << " 'Initialization Completed'";
	//INFO_LOG("FeeModule", fmt::format("{} - Initialization Completed ", __PRETTY_FUNCTION__));
}

void LSL::FeeModule::FeeComissionManager::getAccountRelatedData(OrderExecutionData& data,
	TradeFees& tradeFees, TradeFees& mpidFees)
{
	//Populating with FCM AccoundID GUIDs
	try
	{
		data.accountId = m_accountStore.getAccountId(data.accountValue);
		Account acc = m_accountStore.getAccount(data.accountId);

		data.firmId = acc.firmId;
		data.mpidId = acc.mpidId;
		tradeFees = m_planSelector->GetTradeFees(data.accountId);
		mpidFees = m_planSelector->GetMPIDFees(data.mpidId);
	}
	catch (std::exception &ex)
	{
		//If Account Doesn't Exist populate Id's with firms Id mapped from Props
		//LogWarning() << __PRETTY_FUNCTION__ << "Account Doesn't Exist : Executing via Default Firm - " << data.accountValue;
		//WARNING_LOG("FeeModule", fmt::format("{} - Account Doesn't Exist : Executing via Default Firm:{}", __PRETTY_FUNCTION__, data.accountValue));
		//LogWarning() << __PRETTY_FUNCTION__ << ex.what();
		//WARNING_LOG("FeeModule", fmt::format("{} - {}", __PRETTY_FUNCTION__, ex.what()));

		data.firmId = m_accountStore.getFirmId(data.dfidRecv);
		data.mpidId = m_accountStore.getMpidId(data.mpidRecv);
		tradeFees = m_planSelector->GetTradeFees(data.firmId);
		mpidFees = m_planSelector->GetMPIDFees(data.mpidId);
	}
}

FeeComissionData LSL::FeeModule::FeeComissionManager::CalculateFeeAndComission(OrderExecutionData& data)
{
	// Get the Plan from plan selector
	//LogInfo() << __PRETTY_FUNCTION__ << " 'FCM Calculation Initiated for OrderId:" << data.orderId << " | ExecutionId:" << data.executionId << "'";
	//INFO_LOG("FeeModule", fmt::format("{} - 'FCM Calculation Initiated for OrderId:{} | ExecutionId:{}", __PRETTY_FUNCTION__, data.orderId, data.executionId));
	//LogInfo() << "Data Before Execution : " << data;
	//INFO_LOG("FeeModule", fmt::format("{} - Data Before Execution :{} ", __PRETTY_FUNCTION__, data.getString()));

	FeeComissionData feeCommissionData;
	FeeComissionData mpIdFeeData;

	try
	{
		//PlansMap  plans;
		TradeFees tradeFees, mpidFees;
		boost::algorithm::to_upper(data.accountValue);
		//Populating with FCM AccoundID GUIDs
		getAccountRelatedData(data, tradeFees, mpidFees);

		data.tape = m_symbolStore.getTapeData(data.symbol);
		data.exchangeType = m_symbolStore.getExchangeData(data.symbol);

		switch (data.execTransType)
		{
			case EN_ExecTransType_Cancel:
			{
				data.quantity = -data.quantity;
				data.lastShares = -data.lastShares;
				m_FCStore->FillState(data); //update monthly volume to remove this quantity, fill should be decremented(todo)
				data.quantity = -data.quantity;
				data.lastShares = -data.lastShares;

				//LogDebug() << "Executing bust for TradeFees : ";
				//DEBUG_LOG("FeeModule", fmt::format("{} - Executing bust for TradeFees :", __PRETTY_FUNCTION__));
				feeCommissionData = m_planExecutor->Execute(tradeFees, m_plans, data); //this fee should be substracted from total fee on this order

				//LogDebug() << "Executing bust for MPID Fees : ";
				//DEBUG_LOG("FeeModule", fmt::format("{} - Executing bust for MPID Fees :", __PRETTY_FUNCTION__));
				mpIdFeeData = m_planExecutor->Execute(mpidFees, m_plans, data);

				m_FCStore->insertOrderHistory(data);
			}
			break;

			case EN_ExecTransType_Correct:
			{
				//2 step approach
				//run bust algo
				//run correct algo as new

				//get order from history based on execRefId of data here
				auto orderToCancel = m_FCStore->getOrderHistory(data.orderId + "_" + data.execRefId + "_" + data.accountId);

				//orderId, execId, price, monthly volume, lastShares, quantity, fillCount

				//use that order to run cancel iteration
				orderToCancel.quantity = -orderToCancel.quantity;
				orderToCancel.lastShares = -orderToCancel.lastShares;
				m_FCStore->FillState(orderToCancel);
				orderToCancel.quantity = -orderToCancel.quantity;
				orderToCancel.lastShares = -orderToCancel.lastShares;

				orderToCancel.execTransType = EN_ExecTransType_Cancel;

				feeCommissionData = m_planExecutor->Execute(tradeFees, m_plans, orderToCancel);

				mpIdFeeData = m_planExecutor->Execute(mpidFees, m_plans, orderToCancel);

				//then run new iteration based on current data variable
				m_FCStore->FillState(data);

				//LogDebug() << "Executing correction for TradeFees : ";
				//DEBUG_LOG("FeeModule", fmt::format("{} - Executing correction for TradeFees :", __PRETTY_FUNCTION__));
				feeCommissionData = m_planExecutor->Execute(tradeFees, m_plans, data);

				//LogDebug() << "Executing correction for MPID Fees : ";
				//DEBUG_LOG("FeeModule", fmt::format("{} - Executing correction for MPID Fees :", __PRETTY_FUNCTION__));
				mpIdFeeData = m_planExecutor->Execute(mpidFees, m_plans, data);

				m_FCStore->insertOrderHistory(data);
			}
			break;

		case EN_ExecTransType_New:
		default:
			{
			// Populate with the old states
			m_FCStore->FillState(data);

			// Execute the plan
			//LogDebug() << "Executing for TradeFees : ";
			//DEBUG_LOG("FeeModule", fmt::format("{} - Executing for TradeFees :", __PRETTY_FUNCTION__));
			feeCommissionData = m_planExecutor->Execute(tradeFees, m_plans, data);

			//LogDebug() << "Executing for MPID Fees : ";
			//DEBUG_LOG("FeeModule", fmt::format("{} - Executing for MPID Fees :", __PRETTY_FUNCTION__));
			mpIdFeeData = m_planExecutor->Execute(mpidFees, m_plans, data);

			m_FCStore->insertOrderHistory(data);
			}
			break;
		}

		// Save Fee and comission in DB
		m_FCStore->SaveFeeAndComission(data, feeCommissionData, mpIdFeeData);
		//m_FCStore->SaveFeeAndComission(data, mpIdFeeData);
		//LogInfo() << "Trade Fees - " << feeCommissionData;
		//INFO_LOG("FeeModule", fmt::format("{} - Trade Fees :{} ", __PRETTY_FUNCTION__, feeCommissionData.getStringFeeCommission()));
		//LogInfo() << "MPID Fees - " << mpIdFeeData;
		//INFO_LOG("FeeModule", fmt::format("{} - MPID Fees :{} ", __PRETTY_FUNCTION__, mpIdFeeData.getStringFeeCommission()));
		//LogInfo() << "Data After Execution - " << data;
		//INFO_LOG("FeeModule", fmt::format("{} - Data After Execution :{} ", __PRETTY_FUNCTION__, data.getString()));
	}

	catch (std::exception& ex)
	{
		//LogWarning() << __PRETTY_FUNCTION__ << " '" << ex.what() << "'";
		//WARNING_LOG("FeeModule", fmt::format("{} - {}", __PRETTY_FUNCTION__, ex.what()));
		//LogWarning() << __PRETTY_FUNCTION__ << " 'Saving Default FeeAndCommissionData for ExecutionId:" << data.executionId << "'";
		//WARNING_LOG("FeeModule", fmt::format("{} - Saving Default FeeAndCommissionData for ExecutionId:{}", __PRETTY_FUNCTION__, data.executionId));
		m_FCStore->SaveFeeAndComission(data, feeCommissionData, mpIdFeeData);
	}

	catch (...)
	{
		//LogWarning() << __PRETTY_FUNCTION__ << " 'Exception in Calculating Fees & Commissions'";
		//WARNING_LOG("FeeModule", fmt::format("{} - Exception in Calculating Fees & Commissions", __PRETTY_FUNCTION__));
		//LogWarning() << __PRETTY_FUNCTION__ << " 'Saving Default FeeAndCommissionData for ExecutionId:" << data.executionId << "'";
		//WARNING_LOG("FeeModule", fmt::format("{} - Saving Default FeeAndCommissionData for ExecutionId:{}", __PRETTY_FUNCTION__, data.executionId));
		m_FCStore->SaveFeeAndComission(data, feeCommissionData, mpIdFeeData);
	}
	// Return Fee commission data
	return feeCommissionData;
}

void LSL::FeeModule::FeeComissionManager::finish()
{
	//LogInfo() << __PRETTY_FUNCTION__ << " 'called'";
	//INFO_LOG("FeeModule", fmt::format("{} - called ", __PRETTY_FUNCTION__));

	// finish all the components
	m_FCStore->finish();
	m_planSelector->finish();
	m_planExecutor->finish();
	m_store->finish();
	m_cliManager.finish();
	m_methodInvoker->finish();
	DbDataAdapter::getInstance()->finish();
	//LogInfo() << __PRETTY_FUNCTION__ << " 'finished successfully'";
	//INFO_LOG("FeeModule", fmt::format("{} - finished successfully ", __PRETTY_FUNCTION__));
}

//===================================== BACK DATE =======================================
FeeComissionData LSL::FeeModule::FeeComissionManager::CalculateFeeAndComission(OrderExecutionData& data, Adjustment& adjustment)
{
	// Get the Plan from plan selector
	//LogInfo() << __PRETTY_FUNCTION__ << " 'FCM Calculation Initiated for OrderId:" << data.orderId << " | ExecutionId:" << data.executionId << "'";
	//INFO_LOG("FeeModule", fmt::format("{} - 'FCM Calculation Initiated for OrderId:{} | ExecutionId:{}", __PRETTY_FUNCTION__, data.orderId,data.executionId));

	FeeComissionData feeCommissionData;
	FeeComissionData mpIdFeeData;

	try
	{
		TradeFees tradeFees, mpidFees;
		boost::algorithm::to_upper(data.accountValue);
		//Populating with FCM AccoundID GUIDs
		getAccountRelatedData(data, tradeFees, mpidFees);

		data.tape = m_symbolStore.getTapeData(data.symbol);
		data.exchangeType = m_symbolStore.getExchangeData(data.symbol);

		if (adjustment.adjustmentType != EN_Adjustment_MPIDFee_Mpid && adjustment.adjustmentType != EN_Adjustment_MPIDFee_MultipleMpids)
		{
			//LogDebug() << "Executing for TradeFees : ";
			//DEBUG_LOG("FeeModule", fmt::format("{} - Executing for TradeFees :", __PRETTY_FUNCTION__));
			TradeFees tradeFees = m_planSelector->GetTradeFees(data.accountId);
			feeCommissionData = m_planExecutor->Execute(tradeFees, m_plans, data);
		}

		//LogDebug() << "Executing for MPID Fees : ";
		//DEBUG_LOG("FeeModule", fmt::format("{} - Executing for MPID Fees :", __PRETTY_FUNCTION__));
		TradeFees mpIdFees = m_planSelector->GetMPIDFees(data.mpidId);
		mpIdFeeData = m_planExecutor->Execute(mpIdFees, m_plans, data);

		// Save Fee and comission in DB
		m_FCStore->SaveFeeAndComission(data, feeCommissionData, mpIdFeeData);

		// Populate with the old states
		//m_FCStore->FillState(data);

		//LogInfo() << "Trade Fees - " << feeCommissionData;
		//INFO_LOG("FeeModule", fmt::format("{} - Trade Fees :{} ", __PRETTY_FUNCTION__, feeCommissionData.getStringFeeCommission()));
		//LogInfo() << "MPID Fees - " << mpIdFeeData;
		//INFO_LOG("FeeModule", fmt::format("{} - MPID Fees :{} ", __PRETTY_FUNCTION__, mpIdFeeData.getStringFeeCommission()));
		//LogInfo() << "Data After Execution - " << data;
		//std::ostream stringStream << "'";
		//INFO_LOG("FeeModule", fmt::format("{} - Data After Execution :{} ", __PRETTY_FUNCTION__, data.getString() ) );	
		
	}

	catch (...)
	{
		//LogWarning() << __PRETTY_FUNCTION__ << " 'Exception Saving Default FeeAndCommissionData for ExecutionId:" << data.executionId << "'";
		//WARNING_LOG("FeeModule", fmt::format("{} - Exception Saving Default FeeAndCommissionData for ExecutionId:{}", __PRETTY_FUNCTION__, data.executionId));
		m_FCStore->SaveFeeAndComission(data, feeCommissionData, mpIdFeeData);
	}
	return feeCommissionData;
}

std::string LSL::FeeModule::FeeComissionManager::getEffectiveBusinessDate(const std::string& date, const std::string& time)
{
	return m_holidayStore.getEffectiveBusinessDate(date, time);
}

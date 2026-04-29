#include "../FeeModule/PlanExecutorStore.h"
#include "../FeeModule/DbDataAdapter.h"
#include "../FeeModule/logger/logger.hpp"


LSL::FeeModule::PlanExecutorStore::PlanExecutorStore(bool isEngine) : m_isEngine(isEngine)
{ }

void LSL::FeeModule::PlanExecutorStore::init()
{
	//LogDebug() << __PRETTY_FUNCTION__ << " 'Initializing'";
	//DEBUG_LOG("FeeModule", fmt::format("{} - Initializing", __PRETTY_FUNCTION__));

	if (!m_isEngine)
	{
		std::unordered_map <std::string, int64_t> planTypeMap;

		std::vector<std::vector<std::string>> planTypeVect; // To Release as soon as we are done with this vector
		int error = DbDataAdapter::getInstance()->dataRetrievePlanType(planTypeVect);
		if (error != 0 || planTypeVect.empty())
		{
			//throw std::runtime_error("Unable to Retrieve Plan Types");
		}

		for (const auto& plan : planTypeVect)
		{
			try
			{
				planTypeMap.insert(std::make_pair(plan.at(0), std::stoi(plan.at(1))));
				//PlanID against PlanType
			}
			catch (std::exception& ex)
			{
				//LogWarning() << __PRETTY_FUNCTION__ << ex.what();
				//LogWarning() << __PRETTY_FUNCTION__ << " Unable To Set PlanType Against Plan for:" << plan.at(0);
				//WARNING_LOG("FeeModule", fmt::format("{} -  {}", __PRETTY_FUNCTION__, ex.what()));
				//WARNING_LOG("FeeModule", fmt::format("{} - Unable To Set PlanType Against Plan for: {}", __PRETTY_FUNCTION__, plan.at(0)));
				continue;
			}
		}
		planTypeVect.clear();

		std::vector<std::vector<std::string>> planExecutorData; // To Release as soon as we are done with this vector
		DbDataAdapter::getInstance()->dataRetrieveOrderFees(planExecutorData);

		for (const auto& order : planExecutorData)
		{
			try
			{
				//key -> planId + _ + orderId
				std::string key = order.at(1) + "_" + order.at(0);
				Fee fee = std::stod(order.at(2));
				PlanType planType = static_cast<PlanType>(planTypeMap.at(order.at(1))); //PlanId
				setExecutorStateInternal(planType, key, fee);
			}
			catch (std::exception& ex)
			{
				//LogWarning() << __PRETTY_FUNCTION__ << ex.what();
				//LogWarning() << __PRETTY_FUNCTION__ << " Unable To Set setExecutor State Plan for KEY:" << order.at(0);
				//WARNING_LOG("FeeModule", fmt::format("{} -  {}", __PRETTY_FUNCTION__, ex.what()));
				LogWarning() << __PRETTY_FUNCTION__ << " Unable To Set setExecutor State Plan for KEY & PlanID: " << order.at(0) << " - " << order.at(1);
				continue;
			}
		}
		planExecutorData.clear();
	}
	//LogInfo() << __PRETTY_FUNCTION__ << " 'Initialized'";
	//INFO_LOG("FeeModule", fmt::format("{} - Initialized", __PRETTY_FUNCTION__));
}


Fee LSL::FeeModule::PlanExecutorStore::GetExecutorState(int64_t planId, std::string dataKey)
{
	//LogDebug() << __PRETTY_FUNCTION__ << " 'called'";
	//DEBUG_LOG("FeeModule", fmt::format("{} - called", __PRETTY_FUNCTION__));
	MAP_EXECUTOR_STORE::iterator executorSpecificStore = m_executorStores.find(planId);

	if (executorSpecificStore == m_executorStores.end())
	{
		Fee fee (0);
		return fee;
	}

	MAP_FEE_STORE::iterator it = executorSpecificStore->second.find(dataKey);

	if (it == executorSpecificStore->second.end())
	{
		Fee fee (0);
		return fee;
	}

	return it->second;
}

void LSL::FeeModule::PlanExecutorStore::SetExecutorState(int64_t planType, std::string dataKey, const Fee& fee)
{
	//LogDebug() << __PRETTY_FUNCTION__ << " 'called'";
	//DEBUG_LOG("FeeModule", fmt::format("{} - called", __PRETTY_FUNCTION__));
	setExecutorStateInternal(planType, dataKey, fee);
}

void LSL::FeeModule::PlanExecutorStore::setExecutorStateInternal(int64_t planType, std::string dataKey, const Fee& fee)
{
	//LogDebug() << __PRETTY_FUNCTION__ << " 'called'";
	//DEBUG_LOG("FeeModule", fmt::format("{} - called", __PRETTY_FUNCTION__));
	m_executorStores[planType][dataKey] = fee;
}

void LSL::FeeModule::PlanExecutorStore::finish()
{
	//LogInfo() << __PRETTY_FUNCTION__ << " 'Finished Successfully'";
	//INFO_LOG("FeeModule", fmt::format("{} - Finished Successfully", __PRETTY_FUNCTION__));
}

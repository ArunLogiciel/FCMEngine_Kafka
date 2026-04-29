#include "../FeeModule/PlanSelector.h"
#include "../FeeModule/DbDataAdapter.h"
#include "../FeeModule/logger/logger.hpp"

using TradeFees = std::unordered_map<std::string, TradeFee>;
using PlansMap = std::unordered_map<std::string, Plan>;

void LSL::FeeModule::PlanSelector::init(const std::vector<Account>& accounts)
{
	//LogDebug() << __PRETTY_FUNCTION__ << ": Initializing";
	//DEBUG_LOG("FeeModule", fmt::format("{} - Initializing", __PRETTY_FUNCTION__));

	populatePlans();

	populateTradeFees(accounts);

	//LogInfo() << __PRETTY_FUNCTION__ << " 'Initialized'";
	//INFO_LOG("FeeModule", fmt::format("{} - Initialized", __PRETTY_FUNCTION__));
}

void LSL::FeeModule::PlanSelector::init(Adjustment& adjustment)
{
	//LogDebug() << __PRETTY_FUNCTION__ << ": Initializing";
	//DEBUG_LOG("FeeModule", fmt::format("{} - Initializing", __PRETTY_FUNCTION__));
	
	//Retrieving MPID Accounts Only
	std::vector<Account> accounts;
	DbDataAdapter::getInstance()->RetrieveMpidAccounts(accounts, adjustment.mpidId, adjustment.adjustmentType);
	// Move this to Engine... not here... PLEASE
	//if (accounts.size() > 0) 
	//{
	//	adjustment.mpidId = accounts.at(0).mpidId;
	//}

	populateAdjustedPlans(adjustment);

	populateAdjustedTradeFees(adjustment, accounts);

	//LogInfo() << __PRETTY_FUNCTION__ << " 'Initialized'";
	//INFO_LOG("FeeModule", fmt::format("{} - Initialized", __PRETTY_FUNCTION__));
}

void LSL::FeeModule::PlanSelector::populatePlans()
{
	std::vector<Plan> plans;
	int error = DbDataAdapter::getInstance()->dataRetrievedPlans(plans);
	if (error != 0 || plans.empty())
	{
		//throw std::runtime_error("Unable to Retrieve Plans");
	}
	
	//LogDebug() << __PRETTY_FUNCTION__ << " 'TradeFeePlanExecutor: Inserting Retrieved Plans from DB to MAP'";
	//DEBUG_LOG("FeeModule", fmt::format("{} - TradeFeePlanExecutor: Inserting Retrieved Plans from DB to MAP ", __PRETTY_FUNCTION__));
	for (std::vector<Plan>::iterator it = plans.begin(); it != plans.end(); ++it)
	{
		if (it->feeTypeId == EN_FeeCategoryType_TradeFee ||	it->feeTypeId == EN_FeeCategoryType_MpidFee	) 	// TradeFee = 1, MPIDFee = 3
		{
			m_plans.insert(std::make_pair(it->id, *it));
		}
	}
}

void LSL::FeeModule::PlanSelector::populateTradeFees(const std::vector<Account>& accounts)
{
	std::vector<TradeFee> tradeFees;
	int error = DbDataAdapter::getInstance()->dataRetrievedTradeFeePlan(tradeFees);
	if (error != 0 || tradeFees.empty())
	{
		//throw std::runtime_error("Unable to Retrieve TradeFees");
	}

	//LogDebug() << __PRETTY_FUNCTION__ << " 'Populating Temporary Trade Fee Structs of Account & Group'";
	//DEBUG_LOG("FeeModule", fmt::format("{} - Populating Temporary Trade Fee Structs of Account & Group", __PRETTY_FUNCTION__));

	std::unordered_map<std::string, TradeFees> accountsPlans;
	std::unordered_map<std::string, TradeFees> firmsPlans;

	//Loop through each trade fee and store the trade fee against group and account in map
	for (std::vector<TradeFee>::iterator it = tradeFees.begin(); it != tradeFees.end(); ++it)
	{
		auto planItr = m_plans.find(it->planId);
		if (planItr != m_plans.end())
		{
			auto feeCategory = planItr->second.feeCategoryId;

			if (it->accessType == EN_AccessType_Account )
			{
				accountsPlans[it->accessId].insert(std::make_pair(feeCategory, *it));
			}
			
			if (it->accessType == EN_AccessType_Firm)
			{
				firmsPlans[it->accessId].insert(std::make_pair(feeCategory, *it));
				m_accountPlans[it->accessId].insert(std::make_pair(feeCategory, *it)); //Adding Firm Plans As well
			}

			else if (it->accessType == EN_AccessType_MPID)	//Handling MPID TradeFees Differently
			{
				auto itr = m_mpidPlans.find(it->accessId);
				if (itr == m_mpidPlans.end())
				{
					TradeFees tf;
					tf.insert(std::make_pair(feeCategory, *it));
					m_mpidPlans.insert(std::make_pair(it->accessId, tf));
				}
				else
				{
					itr->second.insert(std::make_pair(feeCategory, *it));
				}
			}
		}
		else
		{
			//LogWarning() << __PRETTY_FUNCTION__ << " 'No Plan For Found For TradeFee id:" << it->id << "'";
			//WARNING_LOG("FeeModule", fmt::format("{} - No Plan For Found For TradeFee id :{}", __PRETTY_FUNCTION__, it->id));
		}
	}

	//LogDebug() << __PRETTY_FUNCTION__ << " 'Populating m_accountPlans with Accounts & TradeFees'";
	//DEBUG_LOG("FeeModule", fmt::format("{} - Populating m_accountPlans with Accounts & TradeFees", __PRETTY_FUNCTION__));
	//std::vector<Account> accounts;
	//DbDataAdapter::getInstance()->dataRetrievedAccounts(accounts);

	// Fill final map that contains the group and account impact combined against the account.
	//LogDebug() << __PRETTY_FUNCTION__ << " 'Populating m_accountPlans with TradeFees according to Account 1st & Group 2nd'";
	//DEBUG_LOG("FeeModule", fmt::format("{} - Populating m_accountPlans with TradeFees according to Account 1st & Group 2nd", __PRETTY_FUNCTION__));
	for (auto it = accounts.begin(); it != accounts.end(); ++it)
	{
		//Populate Firm Plan Initially in m_accountPlan for the said account
		std::unordered_map<std::string, TradeFees>::iterator firmIt = firmsPlans.find(it->firmId);
		if (firmIt != firmsPlans.end())
		{
			m_accountPlans.insert(std::make_pair(it->id, TradeFees(firmIt->second)));
		}
		else
		{
			m_accountPlans.emplace(std::make_pair(it->id, TradeFees()));
		}

		//If AccountPlan Found For Same Account Then Replace Existing Plan From m_accountPlans
		std::unordered_map<std::string, TradeFees>::iterator accIt = accountsPlans.find(it->id);
		if (accIt != accountsPlans.end())
		{
			TradeFees& finaAccFeePlans = m_accountPlans[it->id];
			TradeFees& tempAccFeePlans = accIt->second;
			for (TradeFees::iterator accFeePlanItr = tempAccFeePlans.begin(); accFeePlanItr != tempAccFeePlans.end(); ++accFeePlanItr)
			{
				finaAccFeePlans[accFeePlanItr->first] = accFeePlanItr->second;
			}
		}
	}
}

void  LSL::FeeModule::PlanSelector::populateAdjustedPlans(Adjustment& adjustment)
{
	std::vector<Plan> plans;
	DbDataAdapter::getInstance()->RetrieveRelevantPlans(plans, adjustment);
	//LogDebug() << __PRETTY_FUNCTION__ << " 'TradeFeePlanExecutor: Inserting Retrieved Plans from DB to MAP'";
	//DEBUG_LOG("FeeModule", fmt::format("{} - TradeFeePlanExecutor: Inserting Retrieved Plans from DB to MAP", __PRETTY_FUNCTION__));
	for (std::vector<Plan>::iterator it = plans.begin(); it != plans.end(); ++it)
	{
		if (it->feeTypeId == EN_FeeCategoryType_TradeFee || it->feeTypeId == EN_FeeCategoryType_MpidFee) 	// TradeFee = 1, MPIDFee = 3
		{
			m_plans.insert(std::make_pair(it->id, *it));
			adjustment.planIds.emplace_back(it->id);
		}
	}
}

void LSL::FeeModule::PlanSelector::populateAdjustedTradeFees(Adjustment& adjustment, std::vector<Account>& accounts)
{
	//LogDebug() << __PRETTY_FUNCTION__ << " 'Populating Temporary Trade Fee Structs of Account & Group'";
	//DEBUG_LOG("FeeModule", fmt::format("{} - Populating Temporary Trade Fee Structs of Account & Group", __PRETTY_FUNCTION__));
	std::vector<TradeFee> tradeFees;
	DbDataAdapter::getInstance()->RetrieveRelevantTradeFee(tradeFees, adjustment);

	std::unordered_map<std::string, TradeFees> accountsPlans;
	std::unordered_map<std::string, TradeFees> firmsPlans;

	//Loop through each trade fee and store the trade fee against group and account in map
	for (std::vector<TradeFee>::iterator it = tradeFees.begin(); it != tradeFees.end(); ++it)
	{
		auto planItr = m_plans.find(it->planId);
		if (planItr != m_plans.end())
		{
			auto feeCategory = planItr->second.feeCategoryId;

			if (it->accessType == EN_AccessType_Account)
			{
				accountsPlans[it->accessId].insert(std::make_pair(feeCategory, *it));
			}

			if (it->accessType == EN_AccessType_Firm)
			{
				firmsPlans[it->accessId].insert(std::make_pair(feeCategory, *it));
				m_accountPlans[it->accessId].insert(std::make_pair(feeCategory, *it)); //Adding Firm Plans As well
			}

			else if (it->accessType == EN_AccessType_MPID)	//Handling MPID TradeFees Differently
			{
				auto itr = m_mpidPlans.find(it->accessId);
				if (itr == m_mpidPlans.end())
				{
					TradeFees tf;
					tf.insert(std::make_pair(feeCategory, *it));
					m_mpidPlans.insert(std::make_pair(it->accessId, tf));
				}
				else
				{
					itr->second.insert(std::make_pair(feeCategory, *it));
				}
			}
		}
		else
		{
			//LogWarning() << __PRETTY_FUNCTION__ << " 'No Plan For Found For TradeFee id:" << it->id << "'";
			//WARNING_LOG("FeeModule", fmt::format("{} - No Plan For Found For TradeFee id :{}", __PRETTY_FUNCTION__, it->id));
		}
	}

	// Fill final map that contains the group and account impact combined against the account.
	//LogDebug()s __PRETTY_FUNCTION__ << " 'Populating m_accountPlans with TradeFees according to Account 1st & Group 2nd'";
	//DEBUG_LOG("FeeModule", fmt::format("{} - Populating m_accountPlans with TradeFees according to Account 1st & Group 2nd", __PRETTY_FUNCTION__));
	for (std::vector<Account>::iterator it = accounts.begin(); it != accounts.end(); ++it)
	{
		//Populate Firm Plan Initially in m_accountPlan for the said account
		std::unordered_map<std::string, TradeFees>::iterator firmIt = firmsPlans.find(it->firmId);
		if (firmIt != firmsPlans.end())
		{
			m_accountPlans.insert(std::make_pair(it->id, TradeFees(firmIt->second)));
		}
		else
		{
			m_accountPlans.emplace(std::make_pair(it->id, TradeFees()));
		}

		//If AccountPlan Found For Same Account Then Replace Existing Plan From m_accountPlans
		std::unordered_map<std::string, TradeFees>::iterator accIt = accountsPlans.find(it->id);
		if (accIt != accountsPlans.end())
		{
			TradeFees& finaAccFeePlans = m_accountPlans[it->id];
			TradeFees& tempAccFeePlans = accIt->second;
			for (TradeFees::iterator accFeePlanItr = tempAccFeePlans.begin(); accFeePlanItr != tempAccFeePlans.end(); ++accFeePlanItr)
			{
				finaAccFeePlans[accFeePlanItr->first] = accFeePlanItr->second;
			}
		}
	}
}

void LSL::FeeModule::PlanSelector::finish()
{
	m_accountPlans.clear();
	m_plans.clear();
	//LogInfo() << __PRETTY_FUNCTION__ << " 'Finished Successfully'";
	//INFO_LOG("FeeModule", fmt::format("{} - Finished Successfully", __PRETTY_FUNCTION__));
}


const TradeFees LSL::FeeModule::PlanSelector::GetTradeFees(const std::string& AccountId)
{
	//LogDebug() << __PRETTY_FUNCTION__ << " 'Retrieving Trade Fees Details Related To AccountID " << AccountId << "'";
	//DEBUG_LOG("FeeModule", fmt::format("{} - Retrieving Trade Fees Details Related To AccountID :{}", __PRETTY_FUNCTION__, AccountId));
	TradeFeesMap::iterator it = m_accountPlans.find(AccountId);
	if (it == m_accountPlans.end())
	{
		//LogDebug() << __PRETTY_FUNCTION__ << " 'Couldn't Find Trade Fee Plan for Account: " << AccountId << "'";
		//DEBUG_LOG("FeeModule", fmt::format("{} - Couldn't Find Trade Fee Plan for Account :{}", __PRETTY_FUNCTION__, AccountId));
		return TradeFees();
	}
	//LogDebug() << __PRETTY_FUNCTION__ << " 'Trade Fee Plan Found for Account: " << AccountId << "'";
	//DEBUG_LOG("FeeModule", fmt::format("{} - Trade Fee Plan Found for Account :{}", __PRETTY_FUNCTION__, AccountId));
	return it->second;
}


const PlansMap& LSL::FeeModule::PlanSelector::GetPlans()
{
	//LogDebug() << __PRETTY_FUNCTION__ << " 'Retrieving Plans'";
	//DEBUG_LOG("FeeModule", fmt::format("{} - Retrieving Plans", __PRETTY_FUNCTION__));
	return m_plans;
}

const TradeFees LSL::FeeModule::PlanSelector::GetMPIDFees(const std::string& MpId)
{
	//LogDebug() << __PRETTY_FUNCTION__ << " 'Retrieving MPID TradeFee of :" << MpId << "'";
	//DEBUG_LOG("FeeModule", fmt::format("{} - Retrieving MPID TradeFee of :{}", __PRETTY_FUNCTION__, MpId));

	TradeFeesMap::iterator it = m_mpidPlans.find(MpId);
	if (it == m_mpidPlans.end())
	{
		//LogDebug() << __PRETTY_FUNCTION__ << " 'Couldn't Find Trade Fee for MPID: " << MpId << "'";
		//DEBUG_LOG("FeeModule", fmt::format("{} - Couldn't Find Trade Fee for MPID: {}", __PRETTY_FUNCTION__, MpId));
		return TradeFees();
	}
	//LogDebug() << __PRETTY_FUNCTION__ << " 'Trade Fee Found for MPID: " << MpId << "'";
	//DEBUG_LOG("FeeModule", fmt::format("{} - Trade Fee Found for MPID :{}", __PRETTY_FUNCTION__, MpId));
	return it->second;
}


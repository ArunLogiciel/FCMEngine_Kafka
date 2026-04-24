#include "../FeeModule/AccountsStore.h"
#include "../FeeModule/DbDataAdapter.h"
#include "../FeeModule/logger/logger.hpp"
#include <boost/algorithm/string.hpp>

void AccountsStore::init()
{
	int error = DbDataAdapter::getInstance()->dataRetrievedAccounts(m_accounts);
	if (error != 0 || m_accounts.empty())
	{
		throw std::runtime_error("Unable to Retrieve Accounts");
	}
	populateFirms();
	populateMpids();
	populateAccounts();

	//LogInfo() << __PRETTY_FUNCTION__ << " 'Accounts Store Initialized'";
	INFO_LOG("FeeModule", fmt::format("{} - Accounts Store Initialized", __PRETTY_FUNCTION__));
}

void AccountsStore::init(const Adjustment& adjustment)
{
	DbDataAdapter::getInstance()->RetrieveMpidAccounts(m_accounts, adjustment.mpidId, adjustment.adjustmentType);

	populateFirms();
	populateMpids();
	populateAccounts();

	//LogInfo() << __PRETTY_FUNCTION__ << " 'Accounts Store Initialized'";
	INFO_LOG("FeeModule", fmt::format("{} - Accounts Store Initialized", __PRETTY_FUNCTION__));
}

void AccountsStore::populateAccounts()
{
	for (auto& account : m_accounts)
	{
		try
		{
			AccountMap.insert({ account.id,account });
			ReverseAccountMap.insert({ account.name, account });
		}
		catch (std::exception& ex)
		{
			//LogWarning() << __PRETTY_FUNCTION__ << " 'Unable To Populate Id Against Value:" << ex.what();
			WARNING_LOG("FeeModule", fmt::format("{} - Unable To Populate Id Against Value:{}", __PRETTY_FUNCTION__, ex.what()));
		}
	}
}

void AccountsStore::populateFirms()
{
	std::vector<std::vector<std::string>> firms;
	DbDataAdapter::getInstance()->dataRetrieveFirms(firms);
	std::vector<std::string> boothIds;

	for (auto& firm : firms)
	{
		try
		{
			if (!firm.at(0).empty())
			{
				std::string dfid = firm.at(0);
				dfid = dfid.substr(1, dfid.length() - 2);
				boost::split(boothIds, dfid, boost::is_any_of(","));
				for (auto& boothId : boothIds)
				{
					boost::algorithm::replace_all(boothId, "\"", ""); //Adding Nulls
					FirmMap.insert({ boothId, firm.at(1) });
				}

				boothIds.clear();
			}
		}
		catch (std::exception& ex)
		{
			boothIds.clear();
			//LogWarning() << __PRETTY_FUNCTION__ << " 'Unable To Populate DFID Against FirmId: " << ex.what();
			WARNING_LOG("FeeModule", fmt::format("{} - Unable To Populate DFID Against Value:{}", __PRETTY_FUNCTION__, ex.what()));
		}
	}
}

void AccountsStore::populateMpids()
{
	std::vector<std::vector<std::string>> mpids;
	DbDataAdapter::getInstance()->dataRetrieveMpids(mpids);


	for (auto& mpid : mpids)
	{
		try
		{
			MpidMap.insert({ mpid.at(0), mpid.at(1) });
		}
		catch (std::exception& ex)
		{
			WARNING_LOG("FeeModule", fmt::format("{} - Unable To Populate MPID Against Value:{}", __PRETTY_FUNCTION__, ex.what()));
		}
	}
}

std::string AccountsStore::getAccountId(const std::string& name)
{
	//LogDebug() << __PRETTY_FUNCTION__ << " 'Getting Account Id Against Name: " << name << "'";
	DEBUG_LOG("FeeModule", fmt::format("{} - Getting Account Id Against Name:{}", __PRETTY_FUNCTION__, name));
	try
	{
		return ReverseAccountMap.at(name).id;
	}
	catch (...)
	{
		//LogWarning() << __PRETTY_FUNCTION__ << " 'AccountValue Does Not Exist In Database :" << name << "'";
		WARNING_LOG("FeeModule", fmt::format("{} - AccountValue Does Not Exist In Database:{}", __PRETTY_FUNCTION__, name));
		throw std::exception("Account Does Not Exist");
	}
}

Account AccountsStore::getAccount(const std::string& id)
{
	//LogDebug() << __PRETTY_FUNCTION__ << " 'Getting Account's Data Against Id: " << id << "'";
	DEBUG_LOG("FeeModule", fmt::format("{} - Getting Account Id Against id:{}", __PRETTY_FUNCTION__, id));
	try
	{
		return AccountMap.at(id);
	}
	catch (...)
	{
		//LogWarning() << __PRETTY_FUNCTION__ << " 'AccountId Does Not Exist In Database :" << id << "'";
		WARNING_LOG("FeeModule", fmt::format("{} - AccountId Does Not Exist In Database:{}", __PRETTY_FUNCTION__, id));
		throw std::exception("Account Does Not Exist");
	}
}

std::string AccountsStore::getFirmId(const std::string& name)
{
	//LogDebug() << __PRETTY_FUNCTION__ << " 'Getting Firm Id Against Name: " << name << "'";
	DEBUG_LOG("FeeModule", fmt::format("{} - Getting Firm Id Against Name:{}", __PRETTY_FUNCTION__, name));
	try
	{
		return FirmMap.at(name);
	}
	catch (...)
	{
		//LogWarning() << __PRETTY_FUNCTION__ << " 'Firm Does Not Exist In Database :" << name << "'";
		WARNING_LOG("FeeModule", fmt::format("{} - Firm Does Not Exist In Database:{}", __PRETTY_FUNCTION__, name));
		throw std::exception("Firm Does Not Exist");
	}
}

std::string AccountsStore::getMpidId(const std::string& name)
{
	//LogDebug() << __PRETTY_FUNCTION__ << " 'Getting Mpid Id Against Name: " << name << "'";
	DEBUG_LOG("FeeModule", fmt::format("{} - Getting Mpid Id Against Name:{}", __PRETTY_FUNCTION__, name));
	try
	{
		return MpidMap.at(name);
	}
	catch (...)
	{
		//LogWarning() << __PRETTY_FUNCTION__ << " 'Mpid Does Not Exist In Database :" << name << "'";
		WARNING_LOG("FeeModule", fmt::format("{} - Mpid Does Not Exist In Database:{}", __PRETTY_FUNCTION__, name));
		throw std::exception("Mpid Does Not Exist");
	}
}
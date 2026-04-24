#include "../FeeModule/SymbolStore.h"
#include "../FeeModule/logger/logger.hpp"
//#include "../FeeModule/DbDataAdapter.h"

using namespace LSL::FeeModule;

SymbolStore::SymbolStore(const IniParser& configs, const std::string& date) : m_date(date)
{
	m_apiConfig._host = configs.Get<std::string>("API.Host");
	m_apiConfig._port = configs.Get<std::string>("API.Port");
	m_apiConfig._name = configs.Get<std::string>("API.Name");
	m_apiConfig._key  = configs.Get<std::string>("API.Key");
	m_GiveOTCPriority = configs.Get<int>("API.GiveOTCPriority",1);
}

void temporaryHandlingForBothSymbols(std::unordered_map<std::string, ExchangeType>& exchangeMap, bool giveOTCPriority)
{
	auto prioritedExchangeType = giveOTCPriority ? ExchangeType::EN_ExchangeType_OTC : ExchangeType::EN_ExchangeType_NMS;
	auto prioritedExchangeTypeString = giveOTCPriority ? "ExchangeType::EN_ExchangeType_OTC" : "ExchangeType::EN_ExchangeType_NMS";

	// Iterate over the map
    for (auto& pair : exchangeMap) 
	{
        // Replace value "Both" with "PriorityType"
        if ( pair.second == ExchangeType::EN_ExchangeType_Both ) 
		{
            pair.second = prioritedExchangeType;
        }
    }

	//LogInfo() << __PRETTY_FUNCTION__ << " 'All Both Exchange Type Changed to : " << prioritedExchangeTypeString;
	INFO_LOG("FeeModule", fmt::format("{} - All Both Exchange Type Changed to {}", __PRETTY_FUNCTION__, prioritedExchangeTypeString));

}

void SymbolStore::init()
{
	try
	{
		APIAdapter adapter(m_apiConfig, m_date);
		adapter.getSecurityMastersData(m_TapeMap, m_ExchangeMap);

		// todo:mush(remove this OTC_Priority chaypee in future)
		temporaryHandlingForBothSymbols(m_ExchangeMap, m_GiveOTCPriority);

		//LogInfo() << __PRETTY_FUNCTION__ << " 'Symbol Store Initialized'";
		INFO_LOG("FeeModule", fmt::format("{} - Symbol Store Initialized", __PRETTY_FUNCTION__));
	}
	catch (const std::exception& ex)
	{
		//LogWarning() << __PRETTY_FUNCTION__ << " 'Unable To Populate Security Master Data'";
		//LogWarning() << __PRETTY_FUNCTION__ << " - " << ex.what();
		WARNING_LOG("FeeModule", fmt::format("{} - Unable To Populate Security Master Data", __PRETTY_FUNCTION__));
		WARNING_LOG("FeeModule", fmt::format("{} - {}", __PRETTY_FUNCTION__, ex.what()));
	}
}

ExchangeType SymbolStore::getExchangeData(const std::string& symbol)
{
	try
	{
		return m_ExchangeMap.at(symbol);
	}
	catch (std::exception)
	{
		//LogWarning() << __PRETTY_FUNCTION__ << " 'Symbol Does Not Exist In Database :" << symbol << "'";
		WARNING_LOG("FeeModule", fmt::format("{} - Symbol Does Not Exist In Database :{}", __PRETTY_FUNCTION__, symbol));
		return ExchangeType::EN_ExchangeType_None;
	}
}

std::string SymbolStore::getTapeData(const std::string& symbol)
{
	try
	{
		return m_TapeMap.at(symbol);
	}
	catch (std::exception)
	{
		//LogWarning() << __PRETTY_FUNCTION__ << " 'Symbol Does Not Exist In Database :" << symbol << "'";
		WARNING_LOG("FeeModule", fmt::format("{} - Symbol Does Not Exist In Database :{}", __PRETTY_FUNCTION__, symbol));
		return { "" };
	}
}

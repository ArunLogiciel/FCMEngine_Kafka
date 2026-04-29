// FeeModule.cpp : Defines the functions for the static library.

#include "../FeeModule/FeeModule.h"
#include "../FeeModule/IniParser.h"
#include "../FeeModule/logger//logger.hpp"
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/gregorian/gregorian.hpp>


LSL::FeeCommissionModule::FeeCommissionModule(const std::string& configPath, const std::string& date)
try :
	m_movedParser(configPath),
	m_logger ("FeeModule", m_movedParser.Get<std::string>("Logging.Path"),
	static_cast<logiciel::logger::levels>(m_movedParser.Get<int>("Logging.LogSeverity")),
		m_movedParser.Get<int>("Logging.LogFlag"), true),
	m_feeComissionManager(m_movedParser, GetValidDate(date))
{
	//Left Blank Intentionally
}
catch (std::exception& ex)
{
	//LogCritical() << __PRETTY_FUNCTION__ << " " << ex.what();
	CRITICAL_LOG("FeeModule", fmt::format("{} - {}", __PRETTY_FUNCTION__,ex.what()));
}
catch (...)
{
	//LogCritical() << __PRETTY_FUNCTION__ << " " << "Initialization Failed!";
	CRITICAL_LOG("FeeModule", fmt::format("{} - Initialization Failed!", __PRETTY_FUNCTION__));
}

void LSL::FeeCommissionModule::init()
{

	//LogWarning() << __PRETTY_FUNCTION__ << ": Initializing";
	//WARNING_LOG("FeeModule", fmt::format("{} - Initializing", __PRETTY_FUNCTION__));
	m_feeComissionManager.init();
	//LogWarning() << __PRETTY_FUNCTION__ << ": Initialized Successfully";
	//WARNING_LOG("FeeModule", fmt::format("{} - Initialized Successfully", __PRETTY_FUNCTION__));
}

void LSL::FeeCommissionModule::init(Adjustment& adjustment)
{
	//LogWarning() << __PRETTY_FUNCTION__ << ": Initializing";
	//WARNING_LOG("FeeModule", fmt::format("{} - Initializing", __PRETTY_FUNCTION__));
	m_feeComissionManager.init(adjustment);
	//LogInfo() << __PRETTY_FUNCTION__ << ": Initialized Successfully";
	//INFO_LOG("FeeModule", fmt::format("{} - Initialized Successfully", __PRETTY_FUNCTION__));
}





FeeComissionData LSL::FeeCommissionModule::CalculateFeeAndComission(OrderExecutionData data)
{
	return m_feeComissionManager.CalculateFeeAndComission(data);
}

FeeComissionData LSL::FeeCommissionModule::CalculateFeeAndComission(OrderExecutionData data, Adjustment& adjustment)
{
	return m_feeComissionManager.CalculateFeeAndComission(data, adjustment);
}

void LSL::FeeCommissionModule::finish()
{
	m_feeComissionManager.finish();
}


std::string LSL::FeeCommissionModule::GetValidDate(const std::string& inputDate)
{
	if (inputDate.empty())
	{
		boost::gregorian::date current_date = boost::gregorian::day_clock::local_day();
		auto date = boost::gregorian::to_iso_string(current_date);
		return date;
	}
	else
	{
		return inputDate;
	}
}

std::string LSL::FeeCommissionModule::getEffectiveBusinessDate(const std::string& date, const std::string& time)
{
	return m_feeComissionManager.getEffectiveBusinessDate(date, time);
}

const std::vector<Account>& LSL::FeeCommissionModule::getAccounts() const
{
	return m_feeComissionManager.getAccounts();
}
#include "../FeeModule/HolidayStore.h"
#include "../FeeModule/logger/logger.hpp"
#include "../FeeModule/fmt/include/fmt/format.h"
#include <fstream>
#include <boost/date_time/gregorian/gregorian.hpp>

using namespace LSL::FeeModule;



HolidayStore::HolidayStore(const IniParser& configs)
{
	m_apiConfig._host = configs.Get<std::string>("HolidayAPI.host");
	m_apiConfig._port = configs.Get<std::string>("HolidayAPI.port");
	m_apiConfig._name = configs.Get<std::string>("HolidayAPI.name");
	m_maxRetries = configs.Get<int>("HolidayAPI.maxRetries", 3);
	m_retryDelayMs = configs.Get<int>("HolidayAPI.retryDelayMs", 2000);
}

void HolidayStore::init()
{
	for (int attempt = 1; attempt <= m_maxRetries; ++attempt)
	{
		try
		{
			HolidayAdapter adapter(m_apiConfig);
			adapter.fetchHolidays(m_holidays);

			return; // Success — exit immediately
		}
		catch (const std::exception& ex)
		{
			if (attempt < m_maxRetries)
			{
				std::this_thread::sleep_for(std::chrono::milliseconds(m_retryDelayMs));
			}
		}
	}
}
bool HolidayStore::isHoliday(const std::string& date) const
{
	return m_holidays.count(date) > 0;
}

bool HolidayStore::isWeekend(const std::string& date)
{
	// date: "YYYY-MM-DD"
	int y = std::stoi(date.substr(0, 4));
	int m = std::stoi(date.substr(5, 2));
	int d = std::stoi(date.substr(8, 2));

	boost::gregorian::date dt(y, m, d);
	auto dow = dt.day_of_week();
	return (dow == boost::gregorian::Saturday || dow == boost::gregorian::Sunday);
}

bool HolidayStore::isNonBusinessDay(const std::string& date) const
{
	return isWeekend(date) || isHoliday(date);
}

std::string HolidayStore::advanceToNextBusinessDay(const std::string& date) const
{
	int y = std::stoi(date.substr(0, 4));
	int m = std::stoi(date.substr(5, 2));
	int d = std::stoi(date.substr(8, 2));

	boost::gregorian::date dt(y, m, d);
	dt += boost::gregorian::days(1);

	std::string candidate = boost::gregorian::to_iso_extended_string(dt);
	while (isNonBusinessDay(candidate))
	{
		dt += boost::gregorian::days(1);
		candidate = boost::gregorian::to_iso_extended_string(dt);
	}
	return candidate;
}

std::string HolidayStore::getEffectiveBusinessDate(const std::string& date, const std::string& time) const
{
	std::string effectiveDate = date; // "YYYY-MM-DD"

	// Rule: trades after 8 PM roll to the next calendar day
	if (time >= "20:00:00")
	{
		int y = std::stoi(date.substr(0, 4));
		int m = std::stoi(date.substr(5, 2));
		int d = std::stoi(date.substr(8, 2));

		boost::gregorian::date dt(y, m, d);
		dt += boost::gregorian::days(1);
		effectiveDate = boost::gregorian::to_iso_extended_string(dt);
	}

	// Skip weekends and holidays
	while (isNonBusinessDay(effectiveDate))
	{
		effectiveDate = advanceToNextBusinessDay(effectiveDate);
	}

	return effectiveDate;
}
#include "../FeeModule/HolidayAdapter.h"
#include "../FeeModule/logger/logger.hpp"
//#include "../FeeModule/fmt/include/fmt/format.h"

using namespace LSL::FeeModule;

HolidayAdapter::HolidayAdapter(const HolidayAPIConfig& apiConfig)
	: m_httpClient(apiConfig._host, apiConfig._port)
	, m_apiName(apiConfig._name)
{}

void HolidayAdapter::fetchHolidays(HolidaySet& holidays)
{
	// GET request — no body needed
	auto response = m_httpClient.get(m_apiName);
	processResponse(holidays, response);
}

void HolidayAdapter::processResponse(HolidaySet& holidays, const std::string& response)
{
	rapidjson::Document body;
	body.Parse(response.c_str());

	if (body.HasParseError())
	{
		//throw std::runtime_error("Holiday API: Failed to parse JSON response");
	}

	if (!body.IsArray())
	{
		//throw std::runtime_error("Holiday API: Expected JSON array in response");
	}

	for (rapidjson::SizeType i = 0; i < body.Size(); i++)
	{
		const rapidjson::Value& item = body[i];

		if (item.HasMember("holiday_dt") && item["holiday_dt"].IsString())
		{
			// "2021-01-01T00:00:00" -> extract "2021-01-01"
			std::string fullDate = item["holiday_dt"].GetString();
			std::string dateOnly = fullDate.substr(0, 10); // "YYYY-MM-DD"
			holidays.insert(dateOnly);
		}
	}

	////INFO_LOG("FeeModule", fmt::format("{} - Fetched {} holidays from API", __PRETTY_FUNCTION__, holidays.size()));
}
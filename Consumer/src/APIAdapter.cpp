#include "../../FeeModule/APIAdapter.h"
#include "../../FeeModule/rapidjson/writer.h"
#include "../../FeeModule/rapidjson/stringbuffer.h"

using namespace LSL::FeeModule;

APIAdapter::APIAdapter(const APIConfig& apiConfig, const std::string& date)
	: m_SecurityMasterAPI(apiConfig._host, apiConfig._port)
	, m_dateTime(date + "T00:00:00Z")
	, m_apiName(apiConfig._name)
	, m_apiKey(apiConfig._key)
{}

void APIAdapter::getSecurityMastersData(TapeMap& tapeMap, ExchangeMap& exchangeMap)
{
	const char* bodyTemplate = "{ \"loadOptions\": { \"take\": 0, \"skip\": 0, \"sort\": [ {\"selector\": \"systemDate\",\"desc\": true }]}, \"date\" : \"1970-01-01T00:00:00\", \"APIKey\" : \"string_value_here\"  }";

	rapidjson::Document body;
	body.Parse(bodyTemplate);

	int const take = 1000;
	// Update the values
	body["date"].SetString(m_dateTime.c_str(), body.GetAllocator()); // Update date to "2024-02-20T08:33:04.949Z"
	body["APIKey"].SetString(m_apiKey.c_str(), body.GetAllocator()); // Update APIKey to "value from config file"
	body["loadOptions"]["take"] = take;  // Update take to 5
	int skip = 0;
	bool isCompleted = false;

	while (!isCompleted)
	{
		std::string requestBody;
		getStringFromJson(body, requestBody);
		auto response = m_SecurityMasterAPI.post(m_apiName, requestBody);
		isCompleted = processResponse(tapeMap, exchangeMap, response);
		skip += take;
		body["loadOptions"]["skip"] = skip;  //Update take to 5
	}
}

void APIAdapter::getStringFromJson(rapidjson::Document& document, std::string& ret)
{
	// Write the modified JSON to a string
	rapidjson::StringBuffer buffer;
	rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
	document.Accept(writer);
	ret = buffer.GetString();
}

bool APIAdapter::processResponse(TapeMap& tapeMap, ExchangeMap& exchangeMap, const std::string& response)
{
	bool result = false;
	rapidjson::Document body;
	body.Parse(response.c_str());

	if (!body.HasParseError())
	{
		if (body.HasMember("statusCode") && (200 == body["statusCode"].GetInt()))
		{
			if (body["loadResult"].HasMember("data") && body["loadResult"]["data"].IsArray())
			{
				// Access the "data" array
				const rapidjson::Value& dataArray = body["loadResult"]["data"];

				if (dataArray.Size() > 0)
				{
					for (rapidjson::SizeType i = 0; i < dataArray.Size(); i++)
					{
						const rapidjson::Value& dataItem = dataArray[i];
						// Access individual fields within each item in the array
						auto symbol = dataItem["symbol"].GetString();
						auto exchangeType = dataItem["exchangeType"].GetInt();
						auto tape = dataItem["tape"].GetString();

						tapeMap.insert({ symbol, tape });
						exchangeMap.insert({ symbol, static_cast<ExchangeType>(exchangeType) });
					}
					result = false;
				}
				else
				{
					result = true;
				}
			}

			return result;
		}
	}

	throw std::runtime_error("RESPONSE STATUS CODE IS NOT HTTP-200 [SUCCESS]");
}

/*
	HttpClient http_client("10.0.10.173", "8010");
	std::string body =
		"{ "loadOptions\": { \"take\": 4, \"skip\": 0, \"sort\": [ {\"selector\": \"systemDate\",\"desc\": true }]}, \"date\" : \"2024-02-19T08:33:04.949Z\" }";
	std::string abc = http_client.post("/api/Security/GetAllSecuritiesExchange", body);
*/



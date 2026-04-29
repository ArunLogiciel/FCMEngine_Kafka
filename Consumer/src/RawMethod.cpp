#include "../FeeModule/RawMethod.h"
#include "../FeeModule/ParsingException.h"
#include "../FeeModule/rapidjson/document.h"
#include <boost/algorithm/string.hpp>
#include "../FeeModule/logger/logger.hpp"

//WARNING: This class is not thread-safe.
RawMethod::RawMethod(std::string& formulaJSON)
{
	//LogDebug() << __PRETTY_FUNCTION__ << " 'Initializing Raw Method'";
	//DEBUG_LOG("FeeModule", fmt::format("{} - Initializing Raw Method", __PRETTY_FUNCTION__));
	parse_json(formulaJSON);
	populate_symbol_table();
	auto parsed = parser.compile(expr_string, expression);

	if (!parsed)
	{
		//LogWarning() << __PRETTY_FUNCTION__ << " 'Parser Unable To Compile Raw Formula'";
		//WARNING_LOG("FeeModule", fmt::format("{} - Parser Unable To Compile Raw Formula", __PRETTY_FUNCTION__));
		throw ParsingException("Parser Unable To Compile Raw Formula");
	}
}

RawMethod::RawMethod(RawMethod& rhs)
{
	//LogDebug() << __PRETTY_FUNCTION__ << " 'Copying Raw Method Object'";
	//DEBUG_LOG("FeeModule", fmt::format("{} - Copying Raw Method Object", __PRETTY_FUNCTION__));
	expression = rhs.expression;
}

RawMethod::RawMethod(RawMethod&& rhs) noexcept
{
	//LogDebug() << __PRETTY_FUNCTION__ << " 'Moving Raw Method Object'";
	//DEBUG_LOG("FeeModule", fmt::format("{} - Moving Raw Method Object", __PRETTY_FUNCTION__));
	expression = rhs.expression;
}

std::pair<bool, double> RawMethod::evaluate(const OrderExecutionData& data)
{
	LogDebug() << __PRETTY_FUNCTION__ << " 'Evaluating Fees & Commission'";

	// this is internal variable that is shared by multiple threads must not be manipulated unless we acquire lock
	std::lock_guard<std::mutex> lock(mtx);
	vars.copy(data);
	expression.value();

	//Check to avoid Infinite Values
	if (!std::isfinite(vars.Fees))
	{
		vars.Fees = 0;
	}

	return std::make_pair(true, vars.Fees);
}

void RawMethod::populate_symbol_table()
{
	//LogDebug() << __PRETTY_FUNCTION__ << " 'Populating Symbols For Raw Method Code'";
	//DEBUG_LOG("FeeModule", fmt::format("{} - Populating Symbols For Raw Method Code", __PRETTY_FUNCTION__));
	symbol_table_t symbol_table;
	symbol_table.add_variable("Penny", vars.Penny);
	symbol_table.add_variable("Price", vars.Price);
	symbol_table.add_variable("Quantity", vars.Quantity);
	symbol_table.add_variable("MonthlyVolume", vars.MonthlyVolume);
	symbol_table.add_variable("AfterHours", vars.AfterHours);
	symbol_table.add_variable("BeforeHours", vars.BeforeHours);
	symbol_table.add_variable("Type", vars.Type);
	symbol_table.add_variable("Fee", vars.Fees);
	symbol_table.add_variable("Mult", vars.Multiplier);
	symbol_table.add_stringvar("ExchangeType", vars.ExchangeType);
	symbol_table.add_stringvar("Route", vars.Route);
	symbol_table.add_stringvar("LIQ", vars.LIQ);
	symbol_table.add_stringvar("LOT", vars.LOT);
	symbol_table.add_stringvar("ExecBroker", vars.ExecBroker);
	symbol_table.add_stringvar("Contra", vars.Contra);
	symbol_table.add_stringvar("DST", vars.DST);
	symbol_table.add_stringvar("Tape", vars.Tape);
	symbol_table.add_stringvar("Time", vars.Time);
	symbol_table.add_stringvar("InternalLiq", vars.InternalLiq);
	symbol_table.add_stringvar("InternalRoute", vars.InternalRoute);
	symbol_table.add_stringvar("Side", vars.Side);
	symbol_table.add_stringvar("UnderlyingSymbol", vars.UnderlyingSymbol);	//Newly Added for Supporting PropReports Plan
	symbol_table.add_variable("MPIDMonthlyVolume", vars.MPIdMonthlyVolume);
	symbol_table.add_variable("FirmMonthlyVolume", vars.FirmMonthlyVolume);
	symbol_table.add_stringvar("LastMarket", vars.LastMarket);
	expression.register_symbol_table(symbol_table);
}

void RawMethod::parse_json(std::string& formula)
{
	//LogDebug() << __PRETTY_FUNCTION__ << " Parsing JSON to get Raw Method Code'";
	//DEBUG_LOG("FeeModule", fmt::format("{} - Parsing JSON to get Raw Method Code", __PRETTY_FUNCTION__));

	if (formula == "null")
	{
		//LogWarning() << __PRETTY_FUNCTION__ << " 'JSON Method Is Empty Unable To Parse'";
		//WARNING_LOG("FeeModule", fmt::format("{} - JSON Method Is Empty Unable To Parse", __PRETTY_FUNCTION__));
		throw ParsingException("JSON Method Is Empty Unable To Parse");
	}

	for (size_t i = 0; i < formula.length(); i++)
	{
		if (formula[i] == '\n' || formula[i] == '\r\n' || formula[i] == '\r' || formula[i] == '\n')
		{
			formula[i] = ' ';
		}

		else if ((i < formula.length() - 4) &&
			(formula[i] == '\\' && formula[i + 1] == 'r' && formula[i + 2] == '\\' && formula[i + 3] == 'n'))
		{
			formula[i] = ' ';
			formula.erase(i + 1, 3); // Removing next three chars of \r\n
		}
	}

	rapidjson::Document document;
	document.Parse(formula.c_str());

	if (document.HasParseError())
	{
		//LogWarning() << __PRETTY_FUNCTION__ << " 'JSON Raw Method Parsing Error'";
		//WARNING_LOG("FeeModule", fmt::format("{} - JSON Raw Method Parsing Error", __PRETTY_FUNCTION__));
		throw ParsingException("Failed to Parse Raw Formula");
	}

	if (!document.HasMember("MethodStr"))
	{
		//LogWarning() << __PRETTY_FUNCTION__ << " 'JSON Raw Method Parsing Error'";
		//WARNING_LOG("FeeModule", fmt::format("{} - JSON Raw Method Parsing Error", __PRETTY_FUNCTION__));
		throw ParsingException("Failed To Parse Raw Formula");
	}

	if (!document["MethodStr"].IsNull())
	{
		expr_string = "";
		expr_string = document["MethodStr"].GetString();
		return;
	}
	else
	{
		//LogWarning() << __PRETTY_FUNCTION__ << " 'JSON Formula Parsing Error'";
		//WARNING_LOG("FeeModule", fmt::format("{} - JSON Formula Parsing Error", __PRETTY_FUNCTION__));
		throw ParsingException("Empty Raw Formula");
	}
}

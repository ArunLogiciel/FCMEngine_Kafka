#pragma once
#include "../FeeModule/CodedMethod.h"
#include "../FeeModule/logger/logger.hpp"
#include "../FeeModule/rapidjson/document.h"
#include "../FeeModule/ParsingException.h"
#include "../FeeModule/FeeFunctions/CodedFunctions.h"


FuncEnum  CodedMethod::get_name(std::string func_name)
{
	if (func_name == "TAFplan")
	{
		return EN_FUNC_TAF_PLAN;
	}

	else if (func_name == "vcskECN")
	{
		return EN_FUNC_VCSK_ECN;
	}
	else if (func_name == "vcskCommission")
	{
		return EN_FUNC_VCSK_COMM;
	}
	else if (func_name == "vcshCommission")
	{
		return EN_FUNC_VCSH_COMM;
	}
	else
	{
		return EN_FUNC_INVALID;
	}
}

CodedMethod::CodedMethod(std::string& formulaJSON)
{
	//LogDebug() << __PRETTY_FUNCTION__ << " Parsing JSON to get Coded Method FunctionName'";
	//DEBUG_LOG("FeeModule", fmt::format("{} - Parsing JSON to get Coded Method FunctionName", __PRETTY_FUNCTION__));
	method_ptr.insert({ EN_FUNC_TAF_PLAN, Coded::TAF::TAFplan });
	method_ptr.insert({ EN_FUNC_VCSK_ECN, Coded::VCSK::vcskECN });
	method_ptr.insert({ EN_FUNC_VCSK_COMM,Coded::VCSK::vcskCommission });
	method_ptr.insert({ EN_FUNC_VCSH_COMM,Coded::VCSH::vcshCommission });
	parse_json(formulaJSON);
}

std::pair<bool, double> CodedMethod::evaluate(const OrderExecutionData& data)
{
	OrderExecutionData _data = data;
	FuncEnum func_enum = get_name(method_name);
	if (func_enum == -1)
	{
		return { false,0.0 };
	}
	
	else
	{
		return { true , method_ptr[func_enum](_data) };	//FunctionCall of Coded Function
	}
}

void CodedMethod::parse_json(std::string& formula)
{ 
	if (formula.empty())
	{
		//LogWarning() << __PRETTY_FUNCTION__ << " 'JSON Formula Is Empty'";
		//WARNING_LOG("FeeModule", fmt::format("{} - JSON Formula Is Empty", __PRETTY_FUNCTION__));
		throw ParsingException("Empty Coded Formula");
	}
	
	rapidjson::Document document;
	document.Parse(formula.c_str());

	if (document.HasParseError())
	{
		//LogWarning() << __PRETTY_FUNCTION__ << " 'JSON Formula Parsing Error'";
		//WARNING_LOG("FeeModule", fmt::format("{} - JSON Formula Parsing Error", __PRETTY_FUNCTION__));
		throw ParsingException("Unable to Parse Coded Formula");
	}
	method_name = "";

	if (document.HasMember("MethodName"))
	{
		if(!document["MethodName"].IsNull())
			method_name = document["MethodName"].GetString();
		return;
	}
	else
	{
		//LogWarning() << __PRETTY_FUNCTION__ << " 'JSON Formula Parsing Error'";
		//WARNING_LOG("FeeModule", fmt::format("{} - JSON Formula Parsing Error", __PRETTY_FUNCTION__));
		throw ParsingException("Coded Member Not Found in Formula");
	}
}

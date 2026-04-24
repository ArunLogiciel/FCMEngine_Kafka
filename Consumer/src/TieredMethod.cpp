#pragma once
#include "../FeeModule/TieredMethod.h"
#include "../FeeModule/ParsingException.h"
#include "../FeeModule/logger//logger.hpp"
#include "../FeeModule/Utility.h"

TieredMethod::TieredMethod(std::string formulaJSON)
{
    LogDebug() << __PRETTY_FUNCTION__ << " 'Initializing Tiered Method'";
    parse_json(formulaJSON);
}

std::pair<bool, double> TieredMethod::evaluate(const OrderExecutionData& data)
{
    LogDebug() << __PRETTY_FUNCTION__ << " 'Evaluating Tiered Method'";

    std::pair<bool,double> fee = {};

    switch (tier_plan.type)
    {
        case EN_Tier_Monthly_Volume:
            
            if (tier_plan.isRegressive)
            {
                fee = calculateTieredFeeRegressive(tier_plan.tiers, (data.monthlyVolume - data.quantity), data.quantity);
            }
            else
            {
                fee = calculateTieredFee(tier_plan.tiers, (data.monthlyVolume - data.quantity), data.quantity);
            }
            break;
        case EN_Tier_MPIdMonthly_Volume:

            if (tier_plan.isRegressive)
            {
                fee = calculateTieredFeeRegressive(tier_plan.tiers, (data.mpidMonthlyVolume - data.quantity), data.quantity);
            }
            else
            {
                fee = calculateTieredFee(tier_plan.tiers, (data.mpidMonthlyVolume - data.quantity), data.quantity);
            }
            break;
        case EN_Tier_FirmMonthly_Volume:

            if (tier_plan.isRegressive)
            {
                fee = calculateTieredFeeRegressive(tier_plan.tiers, (data.firmMonthlyVolume - data.quantity), data.quantity);
            }
            else
            {
                fee = calculateTieredFee(tier_plan.tiers, (data.firmMonthlyVolume - data.quantity), data.quantity);
            }
            break;
        case EN_Tier_Price:
            fee = calculateTieredFee(tier_plan.tiers, data.price);
            break;

        case EN_Tier_Quantity:
            fee = calculateTieredFee(tier_plan.tiers, data.quantity);
            break;

        default:
            LogWarning() << __PRETTY_FUNCTION__ << " 'Invalid Tier Type Provided'";
    }
    return fee;
}

void TieredMethod::parse_json(std::string& formula)
{
    LogDebug() << __PRETTY_FUNCTION__ << " 'Parsing JSON For Tiered Method'";
    
    if (formula.empty())
    {
        LogWarning() << __PRETTY_FUNCTION__ << " 'JSON Method Is Empty Unable To Parse'";
        throw ParsingException("JSON Method Is Empty Unable To Parse");
    }

    rapidjson::Document document;
    document.Parse(formula.c_str());

    if (document.HasParseError())
    {
        LogWarning() << __PRETTY_FUNCTION__ << " 'Parsing JSON For Tiered Method'";
        throw ParsingException("Failed To Parse Tiered Formula");
    };

    if (!(document.HasMember("TieredType")) || !(document.HasMember("TieredTable")) || !(document.HasMember("isRegressive")) )    //MR: Added Check for new Bool
    {
        LogWarning() << __PRETTY_FUNCTION__ << " 'JSON Tiered Method Error'";
        throw ParsingException("Failed To Parse Tiered Formula");
    }

    tier_plan.type = static_cast<TierType>(document["TieredType"].GetInt());
    tier_plan.isRegressive = document["isRegressive"].GetBool();// MR: Added To Get Value of bool from JSON
    const rapidjson::Value& json_tiers = document["TieredTable"];
    for (rapidjson::Value::ConstValueIterator value = json_tiers.Begin(); value != json_tiers.End(); ++value)
    {

        const rapidjson::Value& jsonData = *value;
        Tier tier;
        check_members_existence(jsonData);
        tier.sequence = jsonData["SequenceId"].IsNull() ? 0          :  jsonData["SequenceId"].GetInt(); 
        //tier.start    = jsonData["StartRange"].IsNull() ? 0          :  jsonData["StartRange"].GetDouble();   MR: Not Required Anymore
        tier.end      = jsonData["EndRange"].IsNull()   ? INT64_MAX  :  jsonData["EndRange"].GetDouble();
        tier.fee      = jsonData["Fee"].IsNull()        ? 0.0        :  std::stod(jsonData["Fee"].GetString());

        tier_plan.tiers.push_back(tier);
    }
}

void TieredMethod::check_members_existence(const rapidjson::Value& data)
{
    std::vector<std::string> memberStrings = { "SequenceId", "EndRange", "Fee" };    //Removed Check for Removed Member
    for (const auto& member : memberStrings)
    {
        if (!data.HasMember(member.c_str()))
        {
            throw ParsingException("Some Member of JSON Formula Doesn't Exist");
        }
    }
}

std::pair<bool, double> TieredMethod::calculateTieredFeeRegressive(const std::vector<Tier>& tiers, double prevQty, double deltaQty)
{
    double fee = 0.0;
    double rebate = 0.0;
    double newQty = prevQty + deltaQty;

    for (auto tier = tiers.begin(); tier != tiers.end(); ++tier)
    {
        double tierEnd = tier->end;
        double tierRate = tier->fee;

        // --- CASE 1: Entire delta lies inside this tier ---
        if (newQty <= tierEnd)
        {
            fee += deltaQty * tierRate;
            return { true, fee + rebate };
        }

        // --- CASE 2: Crossing this tier boundary ---
        if (Utility::almostLess(prevQty, tierEnd) && Utility::almostGreater(newQty, tierEnd))
        {
            // Quantity inside THIS tier
            double qtyHere = tierEnd - prevQty;
            fee += qtyHere * tierRate;

            // Now handle remaining quantity through next tiers
            auto currTier = tier;
            for (auto nextTier = std::next(tier); nextTier != tiers.end(); currTier = nextTier++)
            {
                double currEnd = currTier->end;
                double currRate = currTier->fee;
                double nextEnd = nextTier->end;
                double nextRate = nextTier->fee;

                // Rebate at boundary
                rebate += currEnd * (nextRate - currRate);

                // Does remaining quantity finish inside this tier?
                if (Utility::almostLessOrEqual(newQty, nextEnd))
                {
                    fee += (newQty - currEnd) * nextRate;
                    return { true, fee + rebate };
                }

                // Otherwise consume full tier chunk
                fee += (nextEnd - currEnd) * nextRate;
            }

            // Should never occur unless tiers malformed
            break;
        }
    }

    return { false, 0 };// Something went wrong
}

//Calculate Tiered for MonthlyVolume based stuff
std::pair<bool, double> TieredMethod::calculateTieredFee(const std::vector<Tier>& tiers, double prevQty, double deltaQty)
{
    if (tiers.empty())
    {
        LogWarning() << __PRETTY_FUNCTION__ << " 'No Tier Sequence Executed Returning Default Fee 0.0'";
        return { false, 0.0 };
    }

    double newQty = prevQty + deltaQty;
    double lastEnd = 0.0;
    double fee = 0.0;

    for (const auto& tier : tiers)
    {
        double tierStart = lastEnd;
        double tierEnd   = tier.end;
        lastEnd = tierEnd;

        // True delta overlap inside this tier:
        double overlapStart = std::max(prevQty, tierStart);
        double overlapEnd   = std::min(newQty, tierEnd);

        if (Utility::almostGreater(overlapEnd, overlapStart))   // delta exists here
        {
            double overlapQty = overlapEnd - overlapStart;
            fee += overlapQty * tier.fee;
        }

        // once we crossed currentTier's end, stop
        if (Utility::almostGreaterOrEqual(tierEnd, newQty))
            break;
    }

    return { true, fee };
}

//Overload for QTY & PRICE
std::pair<bool, double> TieredMethod::calculateTieredFee(const std::vector<Tier>& tiers, double qty)
{
    if (!tiers.empty())
    {
        double fee = 0;
        double remaining = qty;
        double previous_tier_end = 0;

        for (const auto& tier : tiers)
        {
            if (Utility::almostGreater(qty, tier.end))
            {
                fee += (tier.end - previous_tier_end) * tier.fee;
                previous_tier_end = tier.end;
            }
            else
            {
                remaining -= previous_tier_end;
                fee += remaining * tier.fee;
                remaining = 0;
                return { true, fee };
            }
        }
    }
    LogWarning() << __PRETTY_FUNCTION__ << " 'No Tier Sequence Executed Returning Default Fee 0.0'";
    return { false, 0.0 };
}


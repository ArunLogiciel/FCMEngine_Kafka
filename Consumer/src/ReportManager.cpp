#include "../FeeModule/ReportManager.h"
#include "../FeeModule/logger/logger.hpp"
#include "../FeeModule/rapidcsv/src/rapidcsv.h"
#include <filesystem>
#include <regex>
#include "../FeeModule/framework.h"
#include <filesystem>
#define PROP_REPORT_COLUMNS 38

ReportManager::ReportManager(std::vector<std::vector<std::string>>& reportVector, const std::string& reportDirPath, std::vector<std::string> &symVector, const std::string& symbolFilePath)
								: report(reportVector), reportsFileDirectory(reportDirPath), 
								  symbolsData(symVector), symbolFileName(symbolFilePath)
								  
{
}
void ReportManager::ParseAllFilesCDN(const std::string& date,const std::string& filePath) 
{	
	ParseAllFiles(date, filePath);
}
void ReportManager::ParseEODFiles(const std::string& date)
{
	ParseAllFiles(date, reportsFileDirectory);
}

void ReportManager::ParseAllFiles(const std::string& date,const std::string& directory)
{
	try
	{
		auto fileNames = SearchFileNamesWithDate(date, directory);

		namespace fs = std::filesystem;

		for (const auto& file : fileNames)
		{
			ParseCsvFile(reportsFileDirectory + "\\" + file, directory.length());
		}
	}
	catch (const std::exception& e)
	{
		//throw std::runtime_error( fmt::format("{} - {}", __PRETTY_FUNCTION__, e.what()));
	}
}

void ReportManager::ParseCsvFile(const std::string& fileName, size_t length)
{
	try
	{
		rapidcsv::Document doc(fileName);
		auto count = doc.GetRowCount();
		for (size_t i = 0; i < count; i++)
		{
			report.push_back(doc.GetRow<std::string>(i));
			if (doc.GetRow<std::string>(i).size()<PROP_REPORT_COLUMNS)
			{
				//WARNING_LOG("FCM-Engine", fmt::format("{} - Inconsistent rows in Report File", __PRETTY_FUNCTION__));
				//WARNING_LOG("FCM-Engine", fmt::format("{} - Inconsistent rows in Report File", fileName.c_str()));
				//throw std::exception(fmt::format("{} - Inconsistent rows in Report File", __PRETTY_FUNCTION__).c_str());
			}
			(report.end()-1)->push_back(fileName.substr(length)); // Adding FileName At End of Each Record
		}
		doc.Clear();
	}
	
	catch (const std::exception &e)
	{
		//LogWarning() << __PRETTY_FUNCTION__ << e.what();
		//WARNING_LOG("FCM-Engine", fmt::format("{} - {} ", __PRETTY_FUNCTION__,e.what()));
		//throw std::exception(fmt::format("{} - {} ", __PRETTY_FUNCTION__,e.what()).c_str());
	}

	catch (...)
	{
		//LogWarning() << __PRETTY_FUNCTION__ << " 'Failed To Open Report File'";
		//WARNING_LOG("FCM-Engine", fmt::format("{} - Failed To Open Report File", __PRETTY_FUNCTION__));
		//throw std::exception(fmt::format("{} - Failed To Open Report File", __PRETTY_FUNCTION__).c_str());
	}
}
void ReportManager::ParseDropFiles(std::vector<std::string>& fileName)
{
	try
	{
		for (auto& file : fileName)
		{
			rapidcsv::Document doc(file);
			file = std::filesystem::path(file).filename().string();
			auto count = doc.GetRowCount();
			for (size_t i = 0; i < count; i++)
			{
				report.push_back(doc.GetRow<std::string>(i));
				if (doc.GetRow<std::string>(i).size() < PROP_REPORT_COLUMNS)
				{
					//WARNING_LOG("FCM-Engine", fmt::format("{} - Inconsistent rows in Report File", __PRETTY_FUNCTION__));
					//WARNING_LOG("FCM-Engine", fmt::format("{} - Inconsistent rows in Report File", file.c_str()));
					//throw std::exception(fmt::format("{} - Inconsistent rows in Report File", __PRETTY_FUNCTION__).c_str());
				}
				(report.end() - 1)->push_back(file);
				
			}
			
			doc.Clear();
		}
	}

	catch (const std::exception& e)
	{
		//LogWarning() << __PRETTY_FUNCTION__ << e.what();
		//WARNING_LOG("FCM-Engine", fmt::format("{} - {} ", __PRETTY_FUNCTION__, e.what()));
		//throw std::exception(fmt::format("{} - {} ", __PRETTY_FUNCTION__, e.what()).c_str());
	}

	catch (...)
	{
		//LogWarning() << __PRETTY_FUNCTION__ << " 'Failed To Open Report File'";
		//WARNING_LOG("FCM-Engine", fmt::format("{} - Failed To Open Report File", __PRETTY_FUNCTION__));
		//throw std::exception(fmt::format("{} - Failed To Open Report File", __PRETTY_FUNCTION__).c_str());
	}
}

std::vector<std::string> ReportManager::SearchFileNamesWithDate(const std::string& searchStr,const std::string& directoryPathStr) //Regex based implementation Removed because it was slow and this handles both CDN and EOD
{
	std::vector<std::string> fileNames;

	namespace fs = std::filesystem;
	fs::path directoryPath(directoryPathStr);

	if (!fs::exists(directoryPath) || !fs::is_directory(directoryPath))
	{
		//WARNING_LOG("FCM-Engine",fmt::format("Directory not found: {}", directoryPath.string()));
		return fileNames;
	}

	std::error_code ec;
	for (const auto& entry : fs::directory_iterator(directoryPath, ec))
	{
		if (ec)
		{
			//WARNING_LOG("FCM-Engine", fmt::format("Iteration error: {}", ec.message()));
			break;
		}

		if (!entry.is_regular_file()) continue;

		const std::string fileNameStr = entry.path().filename().string();

		// Must be CSV
		if (fileNameStr.size() < 4 || fileNameStr.compare(fileNameStr.size() - 4, 4, ".csv") != 0)
		{
			continue;
		}

		// Remove extension for parsing
		const std::string nameWithoutExt = fileNameStr.substr(0, fileNameStr.size() - 4);

		bool match = false;

		// -------- Scenario 1: YYYYMMDD.csv --------
		if (nameWithoutExt.size() == 8 &&
			std::all_of(nameWithoutExt.begin(),
				nameWithoutExt.end(),
				::isdigit))
		{
			if (nameWithoutExt == searchStr)
				match = true;
		}
		else
		{
			// -------- Scenario 2: MPID_ACCOUNT_YYYYMMDD_TIMESTAMP.csv --------
			if (nameWithoutExt.find(searchStr) != std::string::npos)
				match = true;
		}

		if (match)
			fileNames.emplace_back(fileNameStr);
	}
	return fileNames;
}

void ReportManager::GetSymbolsDataFromFile(const std::string& dateStr, const std::string& searchStr)
{
	ParseSymbolsData(symbolFileName); // Avoiding Test Symbols
}

void ReportManager::ParseSymbolsData(const std::string& fileName)
{
	std::ifstream file(fileName);
	if (file.is_open())
	{
		std::string line;
		std::getline(file, line); //Wasting Headers
		while (std::getline(file, line))
		{
			symbolsData.push_back(line); 
		}
		file.close();
	}		
}

void ReportManager::ClearReports()
{
	report.clear();
}


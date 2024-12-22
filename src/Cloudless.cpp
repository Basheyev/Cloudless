// Cloudless.cpp: определяет точку входа для приложения.
//


#include "Cloudless.h"

#include "CachedFileIO.h"
#include "RecordFileIO.h"
#include <ctime>
#include <iomanip>

using namespace std;
using namespace Cloudless::Storage;


int main()
{
	const char* path = "d://test.bin";
	CachedFileIO cf;
	cf.open(path);
	RecordFileIO rf(cf);

	std::string msg = "My message for binary records storage";
	// Convert timestamp to local time structure
	


	for (uint64_t i = 0; i < 5; i++) {
		std::stringstream ss;
		auto timestamp = std::time(nullptr);
		std::tm* localTime = std::localtime(&timestamp);
		std::string rnd(6 - i, '#');
		ss << msg << " #" << i << " timestamp:" << std::put_time(localTime, "%Y-%m-%d %H:%M:%S") << rnd;
		std::string s = ss.str();
		uint32_t len = (uint32_t) s.size();
		rf.createRecord(s.c_str(), len);
	}

	std::shared_ptr<RecordCursor> rc = rf.getFirstRecord();

	//rc->next();
	rf.removeRecord(rc);
	
	
	if (rc == nullptr) return 0;

	int i = 0;
	char str[128];
	do {
		uint32_t len = rc->getDataLength();
		if (rc->getRecordData(str, len)) {
			str[len] = 0;
			std::cout << "Record: " << i << " Payload: " << str << "\n";
		}
		i++;
	} while (rc->next());
	
	cf.close();

	//std::filesystem::remove(path);

	return 0;
}

#pragma once


#include <iostream>
#include <string>
#include <mutex>
#include <list>

namespace Cloudless {

    namespace Tests {


        class ITestCase {
        public:
            virtual std::string getName() const = 0;
            virtual void init() = 0;
            virtual void execute() = 0;
            virtual bool verify() const = 0;
            virtual void cleanup() = 0;

            void printResult(const char* useCase, bool result) {
                if (useCase == nullptr) return;
                size_t length = strlen(useCase);
                std::string ss;
                ss = ss + "\t" + useCase;
                if (length < 90) {
                    size_t blanksCount = 90 - length;
                    std::string blanks(blanksCount, '.');
                    ss += blanks;
                }
                {
                    std::lock_guard lock(outputLock);
                    std::cout << ss << " " << (result ? "OK" : "FAILED") << "\n";
                }
            }

        protected:
            std::recursive_mutex outputLock;  // TODO: fix multiple threads
            bool finalResult;
        };


        class CloudlessTests {
        public:
            void addTestCase(ITestCase* tc) { testCases.push_back(tc); }
            void run();
        private:
            std::list<ITestCase*> testCases;
        };

    }

}
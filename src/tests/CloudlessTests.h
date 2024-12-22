#pragma once


#include <iostream>
#include <string>
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
#include <iostream>

#include "sys.h"

using namespace std;
// 异常退出
void die(string error_msg) {
    cerr << error_msg << endl;
    cerr << "Exiting..." << endl;
    exit(EXIT_FAILURE);
}

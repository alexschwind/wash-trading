#include <stdlib.h>
#include <stdio.h>

__declspec(dllexport)
int detect_label_wash_trades(const int* buyers,
                             const int* sellers,
                             const double* amounts,
                             int len,
                             double margin,
                             int* result_flags,
                             int num_ids) {

    for (int i = 0; i < len; ++i) {
        result_flags[i] = (buyers[i] == sellers[i]) ? 1 : 0;
    }

    return 0;
}
#include <stdlib.h>
#include <math.h>

__declspec(dllexport)
int detect_label_wash_trades(const int* buyers,
                              const int* sellers,
                              const double* amounts,
                              int len,
                              double margin,
                              int* result_flags,
                              int num_ids) {
    // Allocate balance map and trade amounts
    double* balance_map = (double*)calloc(num_ids, sizeof(double));

    if (!balance_map) {
        free(balance_map);
        return -1; // allocation error
    }

    // Step 1: build balanceMap and track trade amounts
    for (int i = 0; i < len; ++i) {
        double amt = amounts[i];
        balance_map[buyers[i]] += amt;
        balance_map[sellers[i]] -= amt;
        result_flags[i] = 0; // default to not flagged
    }

    // Step 2: reverse iterate
    for (int idx = len - 1; idx >= 1; --idx) {
        // Compute mean trade volume
        double total = 0.0;
        for (int i = 0; i <= idx; ++i)
            total += amounts[i];

        double mean = total / (idx + 1);
        if (mean == 0.0) break;

        // Normalize balances and check if all <= margin
        int within_margin = 1;
        for (int i = 0; i < num_ids; ++i) {
            if (fabs(balance_map[i] / mean) > margin) {
                within_margin = 0;
                break;
            }
        }

        if (within_margin) {
            for (int i = 0; i < idx; ++i) // < idx oder <= idx?
                result_flags[i] = 1;

            free(balance_map);
            return 1;  // Success: found wash trades
        }

        // Otherwise, roll back trade i
        double amt = amounts[idx];
        balance_map[buyers[idx]] -= amt;
        balance_map[sellers[idx]] += amt;
    }

    free(balance_map);
    return 1;
}

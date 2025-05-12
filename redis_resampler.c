// Script to resample ticker data to 5 minute time frame and store it in redis database

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <unistd.h>
#include <stdbool.h>
#include <pthread.h>
#include <hiredis/hiredis.h>
#include <sys/time.h>
#include <ctype.h>

#define MAX_TOKEN_DICT_SIZE 5000
#define MAX_CANDLES 100000
#define MAX_WORKERS 8
#define REDIS_HOST "localhost"
#define REDIS_PORT 6379
#define SOURCE_DB 0         // Database index for raw ticker data
#define RESAMPLED_DB 1      // Database index for resampled data
#define RESAMPLE_INTERVAL 5 // 5-minute intervals

// Struct to hold market data
typedef struct {
    char symbol[50];
    long long timestamp;  // Millisecond timestamp
    double open;
    double high;
    double low;
    double close;
    double volume;
    double ltp;
    int pivot;
    double pointpos;
    double ema10;
    double ema20;
} MarketData;

// Struct to hold token data
typedef struct {
    char name[50];
    char value[20];
} TokenPair;

// Struct to hold thread data
typedef struct {
    TokenPair *tokens;
    int token_count;
    time_t last_run_time;
} ThreadData;

// Global variables
char SOURCE_PREFIX[50] = "";
char RESAMPLED_PREFIX[50] = "";
pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
time_t g_last_run_time = 0;

// Function declarations
char* get_date_prefix();
void load_tokens(TokenPair *tokens, int *token_count);
void setup_resampled_db();
void run_resampler();
void* process_token_batch(void *arg);
int get_data_batch(TokenPair *tokens, int token_count, time_t since_timestamp, MarketData **data, int *data_count);
void prepare_dataframe(MarketData *data, int data_count, MarketData **resampled_data, int *resampled_count);
void update_pivot_values(const char *token_name, const char *token_value, redisContext *redis_context);
int optimize_pivot_calculations(MarketData *data, int size, int *pivot_values);
int compare_market_data_by_timestamp(const void *a, const void *b);
void calculate_ema(double *prices, int size, int period, double *output);
void calculate_ongoing_ema(double *prices, int size, int period, double last_ema, double *output);
double pointpos(MarketData data);
int store_resampled_data(MarketData *resampled_data, int count, const char *token_name, const char *token_value, redisContext *redis_context);
time_t get_next_run_time(int interval_minutes);
bool is_market_open();
void clear_resampled_database();
void print_token_data(const char *token_name, const char *token_value);
redisContext* redis_connect(int db);

/**
 * Get current date prefix in format "ddmmmyyyy"
 */
char* get_date_prefix() {
    time_t now = time(NULL);
    struct tm *tm_info = localtime(&now);
    static char date_prefix[20];
    
    char month[4];
    strftime(month, sizeof(month), "%b", tm_info);
    
    // Convert month to lowercase
    for (int i = 0; month[i]; i++) {
        month[i] = tolower(month[i]);
    }
    
    sprintf(date_prefix, "%d%s%d", tm_info->tm_mday, month, tm_info->tm_year + 1900);
    return date_prefix;
}

/**
 * Load token dictionary from file or hardcode for testing
 */
// void load_tokens(TokenPair *tokens, int *token_count) {
//     // For this implementation, we'll just hardcode a test token
//     // In a full implementation, you would load from a file
//     *token_count = 1;
//     strcpy(tokens[0].name, "DIXON25MAY15000PE");
//     strcpy(tokens[0].value, "80011");
    
//     printf("Loaded %d tokens\n", *token_count);
// }

void load_tokens(TokenPair *tokens, int *token_count) {
    time_t now;
    struct tm *tm_info;
    char filename[256], date_str[32];
    FILE *file;
    
    time(&now);
    tm_info = localtime(&now);
    
    // Format date for filename
    char month[4];
    switch(tm_info->tm_mon) {
        case 0: strcpy(month, "jan"); break;
        case 1: strcpy(month, "feb"); break;
        case 2: strcpy(month, "mar"); break;
        case 3: strcpy(month, "apr"); break;
        case 4: strcpy(month, "may"); break;
        case 5: strcpy(month, "jun"); break;
        case 6: strcpy(month, "jul"); break;
        case 7: strcpy(month, "aug"); break;
        case 8: strcpy(month, "sep"); break;
        case 9: strcpy(month, "oct"); break;
        case 10: strcpy(month, "nov"); break;
        case 11: strcpy(month, "dec"); break;
    }
    
    snprintf(date_str, sizeof(date_str), "%s%d%d", month, tm_info->tm_mday, tm_info->tm_year + 1900);
    snprintf(filename, sizeof(filename), "tokens/%stoken.txt", date_str);
    
    printf("Looking for token file: %s\n", filename);
    
    file = fopen(filename, "r");
    if (!file) {
        printf("Token file not found, searching for most recent file\n");
        
        // Try a few recent dates
        for (int i = 1; i <= 5; i++) {
            time_t prev_day = now - (i * 86400);
            struct tm *prev_tm = localtime(&prev_day);
            
            char prev_month[4];
            switch(prev_tm->tm_mon) {
                case 0: strcpy(prev_month, "jan"); break;
                case 1: strcpy(prev_month, "feb"); break;
                case 2: strcpy(prev_month, "mar"); break;
                case 3: strcpy(prev_month, "apr"); break;
                case 4: strcpy(prev_month, "may"); break;
                case 5: strcpy(prev_month, "jun"); break;
                case 6: strcpy(prev_month, "jul"); break;
                case 7: strcpy(prev_month, "aug"); break;
                case 8: strcpy(prev_month, "sep"); break;
                case 9: strcpy(prev_month, "oct"); break;
                case 10: strcpy(prev_month, "nov"); break;
                case 11: strcpy(prev_month, "dec"); break;
            }
            
            snprintf(date_str, sizeof(date_str), "%s%d%d", prev_month, prev_tm->tm_mday, prev_tm->tm_year + 1900);
            snprintf(filename, sizeof(filename), "tokens/%stoken.txt", date_str);
            
            file = fopen(filename, "r");
            if (file) {
                printf("Using token file: %s\n", filename);
                break;
            }
        }
        
        if (!file) {
            printf("No token file found. Using default test token.\n");
            // Use hardcoded token for testing
            *token_count = 1;
            strcpy(tokens[0].name, "SOLARINDS25MAY13000PE");
            strcpy(tokens[0].value, "48846");
            return;
        }
    }
    
    // Read the entire file content into a single string
    char fileContent[1024 * 1024] = {0}; // 1MB buffer for file content
    size_t bytesRead = fread(fileContent, 1, sizeof(fileContent) - 1, file);
    fileContent[bytesRead] = '\0';
    fclose(file);
    
    // Parse JSON to extract tokens
    *token_count = 0;
    char *json = fileContent;
    
    // Skip whitespace to find opening brace
    while (*json && isspace(*json)) json++;
    
    if (*json != '{') {
        printf("Invalid JSON format - missing opening brace\n");
        // Use default token as fallback
        *token_count = 0;        
        return;
    }
    
    json++; // Skip opening brace
    
    // Parse each key-value pair
    while (*json && *json != '}' && *token_count < MAX_TOKEN_DICT_SIZE) {
        // Skip whitespace
        while (*json && isspace(*json)) json++;
        
        if (*json != '"') {
            // If not a quote, skip to next quote or end
            while (*json && *json != '"' && *json != '}') json++;
            if (*json == '}' || *json == '\0') break;
        }
        
        json++; // Skip opening quote for key
        
        // Extract key
        char *key_start = json;
        while (*json && *json != '"') json++;
        
        if (*json != '"') {
            printf("Invalid JSON format - missing closing quote for key\n");
            break;
        }
        
        size_t key_len = json - key_start;
        if (key_len >= sizeof(tokens[*token_count].name)) {
            key_len = sizeof(tokens[*token_count].name) - 1;
        }
        
        strncpy(tokens[*token_count].name, key_start, key_len);
        tokens[*token_count].name[key_len] = '\0';
        
        json++; // Skip closing quote for key
        
        // Skip to colon
        while (*json && *json != ':') json++;
        if (*json != ':') {
            printf("Invalid JSON format - missing colon\n");
            break;
        }
        
        json++; // Skip colon
        
        // Skip whitespace
        while (*json && isspace(*json)) json++;
        
        // Check if the value is "null"
        if (*json == 'n' && strncmp(json, "null", 4) == 0) {
            // Skip this token
            json += 4; // Skip over "null"
            
            // Skip to comma or closing brace
            while (*json && *json != ',' && *json != '}') json++;
            
            if (*json == ',') {
                json++; // Skip comma
            }
            
            printf("Skipping token with null value\n");
            continue; // Skip to next iteration of while loop
        }
        
        if (*json != '"') {
            printf("Invalid JSON format - missing opening quote for value\n");
            break;
        }
        
        json++; // Skip opening quote for value
        
        // Extract value
        char *value_start = json;
        while (*json && *json != '"') json++;
        
        if (*json != '"') {
            printf("Invalid JSON format - missing closing quote for value\n");
            break;
        }
        
        size_t value_len = json - value_start;
        if (value_len >= sizeof(tokens[*token_count].value)) {
            value_len = sizeof(tokens[*token_count].value) - 1;
        }
        
        strncpy(tokens[*token_count].value, value_start, value_len);
        tokens[*token_count].value[value_len] = '\0';
        
        json++; // Skip closing quote for value
        
        // Skip to comma or closing brace
        while (*json && *json != ',' && *json != '}') json++;
        
        if (*json == ',') {
            json++; // Skip comma
        } else if (*json != '}') {
            printf("Invalid JSON format - missing comma or closing brace\n");
            break;
        }
        
        (*token_count)++;
    }
    
    printf("Successfully loaded %d tokens from %s\n", *token_count, filename);
    
}
/**
 * Set up the resampled Redis database with metadata
 */
void setup_resampled_db() {
    redisContext *redis = redis_connect(RESAMPLED_DB);
    if (redis == NULL) {
        return;
    }
    
    // Create info key with metadata
    char info_key[100];
    sprintf(info_key, "%s:info", RESAMPLED_PREFIX);
    
    redisReply *reply = redisCommand(redis, "EXISTS %s", info_key);
    if (reply && reply->integer == 0) {
        // Create key with metadata
        redisCommand(redis, "HSET %s description %s", info_key, 
                     "Resampled 5Min market data");
        redisCommand(redis, "HSET %s structure %s", info_key, 
                     "prefix:{token}:{timestamp} -> Hash with OHLCV data");
        redisCommand(redis, "HSET %s fields %s", info_key, 
                     "symbol,timestamp,open,high,low,close,adjVol,ltp");
        redisCommand(redis, "HSET %s pivot_index_key %s", info_key, 
                     "prefix:pivot_index:{token_name}");
        redisCommand(redis, "HSET %s source_db %s", info_key, SOURCE_PREFIX);
        
        time_t now = time(NULL);
        char time_str[30];
        strftime(time_str, sizeof(time_str), "%Y-%m-%d %H:%M:%S", localtime(&now));
        redisCommand(redis, "HSET %s created_at %s", info_key, time_str);
        
        printf("Created resampled DB structure documentation\n");
    }
    
    if (reply) freeReplyObject(reply);
    
    // Create pivot index info key
    char pivot_info_key[100];
    sprintf(pivot_info_key, "%s:pivot_index:info", RESAMPLED_PREFIX);
    
    reply = redisCommand(redis, "EXISTS %s", pivot_info_key);
    if (reply && reply->integer == 0) {
        redisCommand(redis, "HSET %s description %s", pivot_info_key, 
                     "Latest pivot candle indices for each token");
        redisCommand(redis, "HSET %s structure %s", pivot_info_key, 
                     "prefix:pivot_index:{token_name} -> Hash with pivot information");
        redisCommand(redis, "HSET %s fields %s", pivot_info_key, 
                     "token_name,pivot_index,timestamp,last_updated");
        
        time_t now = time(NULL);
        char time_str[30];
        strftime(time_str, sizeof(time_str), "%Y-%m-%d %H:%M:%S", localtime(&now));
        redisCommand(redis, "HSET %s created_at %s", pivot_info_key, time_str);
        
        printf("Created pivot index structure documentation\n");
    }
    
    if (reply) freeReplyObject(reply);
    redisFree(redis);
}

/**
 * Connect to Redis with the specified database
 */
redisContext* redis_connect(int db) {
    redisContext *redis = redisConnect(REDIS_HOST, REDIS_PORT);
    if (redis == NULL || redis->err) {
        if (redis) {
            printf("Error connecting to Redis: %s\n", redis->errstr);
            redisFree(redis);
        } else {
            printf("Cannot allocate redis context\n");
        }
        return NULL;
    }
    
    redisReply *reply = redisCommand(redis, "SELECT %d", db);
    if (reply) freeReplyObject(reply);
    
    return redis;
}

/**
 * Calculate next run time based on interval
 */
time_t get_next_run_time(int interval_minutes) {
    time_t now = time(NULL);
    struct tm *tm_info = localtime(&now);
    
    // Calculate the next interval mark
    int minutes = tm_info->tm_min;
    int next_interval = ((minutes / interval_minutes) + 1) * interval_minutes;
    
    if (next_interval >= 60) {
        tm_info->tm_hour += 1;
        next_interval = 0;
    }
    
    tm_info->tm_min = next_interval;
    tm_info->tm_sec = 0;
    
    time_t next_run = mktime(tm_info);
    
    // Ensure we wait at least 10 seconds
    if (next_run - now < 10) {
        next_run += 60 * interval_minutes;
    }
    
    return next_run;
}

/**
 * Check if market is open
 */
bool is_market_open() {
    time_t now = time(NULL);
    struct tm *tm_info = localtime(&now);
    
    // Check if it's a weekend
    if (tm_info->tm_wday == 0 || tm_info->tm_wday == 6) {
        return false;
    }
    
    // Create time values for market hours
    struct tm start_tm = *tm_info;
    start_tm.tm_hour = 9;
    start_tm.tm_min = 15;
    start_tm.tm_sec = 0;
    
    struct tm end_tm = *tm_info;
    end_tm.tm_hour = 15;
    end_tm.tm_min = 30;
    end_tm.tm_sec = 0;
    
    time_t market_start = mktime(&start_tm);
    time_t market_end = mktime(&end_tm);
    
    return (now >= market_start && now <= market_end);
}

/**
 * Get batch of data for tokens
 */
int get_data_batch(TokenPair *tokens, int token_count, time_t since_timestamp, MarketData **data, int *data_count) {
    redisContext *redis = redis_connect(SOURCE_DB);
    if (redis == NULL) {
        return -1;
    }
    
    // Allocate memory for return data
    *data = malloc(MAX_CANDLES * sizeof(MarketData));
    if (*data == NULL) {
        fprintf(stderr, "Error: Failed to allocate memory for data array\n");
        return -1;  // Return error code
    }
    *data_count = 0;
    
    long long redis_timestamp = (long long)since_timestamp * 1000;
    
    // First, get all the keys in a pipeline
    for (int i = 0; i < token_count; i++) {
        char sorted_key[100];
        sprintf(sorted_key, "%s:%s:index", SOURCE_PREFIX, tokens[i].value);
        
        // Queue EXISTS command
        redisAppendCommand(redis, "EXISTS %s", sorted_key);
    }
    
    // Get EXISTS replies and queue ZRANGE/ZRANGEBYSCORE commands
    for (int i = 0; i < token_count; i++) {
        redisReply *exists_reply;
        if (redisGetReply(redis, (void**)&exists_reply) != REDIS_OK || !exists_reply || exists_reply->integer == 0) {
            if (exists_reply) freeReplyObject(exists_reply);
            continue;
        }
        freeReplyObject(exists_reply);
        
        char sorted_key[100];
        sprintf(sorted_key, "%s:%s:index", SOURCE_PREFIX, tokens[i].value);
        
        // Queue ZRANGE command
        if (since_timestamp > 0) {
            redisAppendCommand(redis, "ZRANGEBYSCORE %s %lld +inf", sorted_key, redis_timestamp);
        } else {
            redisAppendCommand(redis, "ZRANGE %s 0 -1", sorted_key);
        }
    }
    
    // Process ZRANGE/ZRANGEBYSCORE replies and queue HGETALL commands
    for (int i = 0; i < token_count; i++) {
        redisReply *keys_reply;
        if (redisGetReply(redis, (void**)&keys_reply) != REDIS_OK || 
            !keys_reply || keys_reply->type != REDIS_REPLY_ARRAY) {
            if (keys_reply) freeReplyObject(keys_reply);
            continue;
        }
        
        // Queue HGETALL commands for each key
        for (size_t j = 0; j < keys_reply->elements && *data_count < MAX_CANDLES; j++) {
            redisAppendCommand(redis, "HGETALL %s", keys_reply->element[j]->str);
        }
        
        // Process each key's data
        for (size_t j = 0; j < keys_reply->elements && *data_count < MAX_CANDLES; j++) {
            redisReply *data_reply;
            if (redisGetReply(redis, (void**)&data_reply) != REDIS_OK) {
                continue;
            }
            
            if (data_reply && data_reply->type == REDIS_REPLY_ARRAY && data_reply->elements >= 2) {
                MarketData candle;
                memset(&candle, 0, sizeof(MarketData));
                strcpy(candle.symbol, tokens[i].name);
                
                // Process fields
                for (size_t k = 0; k < data_reply->elements; k += 2) {
                    char *field = data_reply->element[k]->str;
                    char *value = data_reply->element[k+1]->str;
                    
                    if (strcmp(field, "timestamp") == 0) {
                        candle.timestamp = atoll(value);
                    } else if (strcmp(field, "open") == 0) {
                        candle.open = atof(value);
                    } else if (strcmp(field, "high") == 0) {
                        candle.high = atof(value);
                    } else if (strcmp(field, "low") == 0) {
                        candle.low = atof(value);
                    } else if (strcmp(field, "close") == 0) {
                        candle.close = atof(value);
                    } else if (strcmp(field, "volume") == 0 || strcmp(field, "adjVol") == 0) {
                        candle.volume = atof(value);
                    } else if (strcmp(field, "ltp") == 0) {
                        candle.ltp = atof(value);
                    }
                }
                
                // Add to result array
                (*data)[*data_count] = candle;
                (*data_count)++;
            }
            
            if (data_reply) freeReplyObject(data_reply);
        }
        
        freeReplyObject(keys_reply);
    }
    
    redisFree(redis);
    return 0;
}

/**
 * Optimize pivot calculations using sliding window
 */
int optimize_pivot_calculations(MarketData *data, int size, int *pivot_values) {
    const int lookback = 3;
    const int lookforward = 3;
    
    // Initialize all pivot values to zero
    memset(pivot_values, 0, size * sizeof(int));
    
    // We need at least 7 candles to find a pivot (3 before + pivot + 3 after)
    if (size < lookback + 1 + lookforward) {
        return 0;
    }
    
    // Compute pivots with direct comparisons - simple and efficient
    for (int i = lookback; i < size - lookforward; i++) {
        double current_high = data[i].high;
        bool is_pivot_high = true;
        
        // Check if current high is greater than all bars in the lookback window
        for (int j = i - lookback; j < i && is_pivot_high; j++) {
            if (current_high <= data[j].high) {
                is_pivot_high = false;
                // Early termination - no need to continue checking
            }
        }
        
        // If still a potential pivot, check against the lookforward window
        if (is_pivot_high) {
            for (int j = i + 1; j <= i + lookforward && is_pivot_high; j++) {
                if (current_high <= data[j].high) {
                    is_pivot_high = false;
                    // Early termination
                }
            }
            
            // If it passed both checks, it's a pivot high
            if (is_pivot_high) {
                pivot_values[i] = 2;  // 2 indicates a pivot high
            }
        }
    }
    
    return 0;
}

/**
 * Calculate EMA using recursive formula
 */
void calculate_ema(double *prices, int size, int period, double *output) {
    if (size < period) {
        // Not enough data for proper EMA
        for (int i = 0; i < size; i++) {
            output[i] = 0.00;
        }
        return;
    }
    
    // Initialize with SMA for the first period value
    double sum = 0;
    for (int i = 0; i < period; i++) {
        sum += prices[i];
    }
    output[period-1] = sum / period;
    
    // Fill with NaN before the period
    for (int i = 0; i < period-1; i++) {
        output[i] = 0.00;
    }
    
    // Calculate EMA using the recursive formula
    double alpha = 2.0 / (period + 1.0);
    for (int i = period; i < size; i++) {
        output[i] = prices[i] * alpha + output[i-1] * (1.0 - alpha);
    }
}

/**
 * Calculate EMA with continuity from last known value
 */
void calculate_ongoing_ema(double *prices, int size, int period, double last_ema, double *output) {
    double alpha = 2.0 / (period + 1.0);
    
    if (last_ema > 0) {
        // Use last known EMA value for first calculation
        output[0] = prices[0] * alpha + last_ema * (1.0 - alpha);
        
        // Calculate remaining EMAs
        for (int i = 1; i < size; i++) {
            output[i] = prices[i] * alpha + output[i-1] * (1.0 - alpha);
        }
    } else {
        // Standard initialization if no seed value
        calculate_ema(prices, size, period, output);
    }
}

/**
 * Calculate pointpos for visualization
 */
double pointpos(MarketData data) {
    if (data.pivot == 1) {
        return data.low - 0.001;
    } else if (data.pivot == 2) {
        return data.high + 0.001;
    } else {
        return NAN;
    }
}

// Define comparison function for sorting by timestamp
int compare_market_data_by_timestamp(const void *a, const void *b) {
    const MarketData *da = (const MarketData *)a;
    const MarketData *db = (const MarketData *)b;
    if (da->timestamp > db->timestamp) return 1;
    if (da->timestamp < db->timestamp) return -1;
    return 0;
}
/**
 * Resample market data to 5-minute intervals
//  */

void prepare_dataframe(MarketData *data, int data_count, MarketData **resampled_data, int *resampled_count) {
    if (data_count == 0) {
        *resampled_count = 0;
        *resampled_data = NULL;
        return;
    }
    
    // Sort data by timestamp (ascending order)
    qsort(data, data_count, sizeof(MarketData), compare_market_data_by_timestamp);

    // Find the earliest and latest timestamps
    long long earliest_ts = data[0].timestamp;
    long long latest_ts = data[data_count-1].timestamp;
    
    // Calculate how many 5-minute intervals we need
    time_t earliest_time = earliest_ts / 1000;
    time_t latest_time = latest_ts / 1000;
    struct tm *tm_earliest = localtime(&earliest_time);
    
    // Align to 5-minute intervals
    tm_earliest->tm_min = (tm_earliest->tm_min / RESAMPLE_INTERVAL) * RESAMPLE_INTERVAL;
    tm_earliest->tm_sec = 0;
    time_t aligned_start = mktime(tm_earliest);
    
    // Calculate the number of intervals
    int num_intervals = ((latest_time - aligned_start) / 60 / RESAMPLE_INTERVAL) + 2;
    
    // Allocate memory for resampled data
    *resampled_data = malloc(num_intervals * sizeof(MarketData));
    if (*resampled_data == NULL) {
        printf("Failed to allocate memory for resampled data\n");
        *resampled_count = 0;
        return;
    }
    
    // Initialize resampled data array
    memset(*resampled_data, 0, num_intervals * sizeof(MarketData));
    
    // For each interval, calculate OHLCV
    int interval_idx = 0;
    
    for (time_t interval_start = aligned_start; 
         interval_start <= latest_time; 
         interval_start += RESAMPLE_INTERVAL * 60) {
        
        long long interval_start_ms = (long long)interval_start * 1000;
        long long interval_end_ms = interval_start_ms + (RESAMPLE_INTERVAL * 60 * 1000);
        
        // Skip the 9:05 and 9:10 intervals (these should be before 9:15 market open)
        struct tm *tm_interval = localtime(&interval_start);
        if (tm_interval->tm_hour == 9 && (tm_interval->tm_min == 5 || tm_interval->tm_min == 10)) {
            continue;
        }
        
        MarketData *candle = &((*resampled_data)[interval_idx]);
        strcpy(candle->symbol, data[0].symbol);
        candle->timestamp = interval_start_ms;
        
        // Improved volume calculation with true interval volume
        double open = 0, high = 0, low = 0, close = 0;
        double interval_volume = 0.0;
        bool first_value = true;
        bool found_data_in_interval = false;
        int first_idx_in_interval = -1;
        int last_idx_in_interval = -1;

        // Find data points within this interval
        for (int i = 0; i < data_count; i++) {
            if (data[i].timestamp >= interval_start_ms && data[i].timestamp < interval_end_ms) {
                if (first_idx_in_interval == -1) {
                    first_idx_in_interval = i;
                }
                last_idx_in_interval = i;
                
                // Initialize OHLC values on first point in interval
                if (first_value) {
                    open = data[i].open;
                    high = data[i].high;
                    low = data[i].low;
                    first_value = false;
                    found_data_in_interval = true;
                } else {
                    // Only compare high/low when needed
                    if (data[i].high > high) high = data[i].high;
                    if (data[i].low < low) low = data[i].low;
                }
                
                // Always update close with the latest value in the interval
                close = data[i].close;
            } else if (data[i].timestamp >= interval_end_ms) {
                break;  // Exit early once we're past the current interval
            }
        }

        // Only create a candle if we have data for this interval
        if (found_data_in_interval) {
            // Calculate volume as the difference between last and first point in the interval
            if (first_idx_in_interval > 0 && last_idx_in_interval >= first_idx_in_interval) {
                // We have previous data point to calculate delta from
                interval_volume = data[last_idx_in_interval].volume - data[first_idx_in_interval-1].volume;
                
                // Handle potential day boundary (volume resets) or data anomalies
                if (interval_volume < 0) {
                    // If we get a negative volume, just use the total volume in the interval
                    interval_volume = data[last_idx_in_interval].volume - data[first_idx_in_interval].volume;
                    
                    // If still negative, fallback to end volume (for extreme cases)
                    if (interval_volume < 0) {
                        interval_volume = data[last_idx_in_interval].volume;
                    }
                }
            } else {
                // First point in the dataset or isolated candle, use volume delta within interval
                if (last_idx_in_interval > first_idx_in_interval) {
                    interval_volume = data[last_idx_in_interval].volume - data[first_idx_in_interval].volume;
                } else {
                    // Single data point in interval, use its volume
                    interval_volume = data[first_idx_in_interval].volume;
                }
            }
            
            // Assign OHLCV values
            candle->open = open;
            candle->high = high;
            candle->low = low;
            candle->close = close;
            candle->volume = interval_volume > 0 ? interval_volume : 0; // Ensure non-negative
            candle->ltp = close;
            
            interval_idx++;
        }
    }
    
    *resampled_count = interval_idx;
    
    // Handle zeros by replacing with the close value
    for (int i = 0; i < *resampled_count; i++) {
        // Replace zeros with the corresponding close value
        if ((*resampled_data)[i].open == 0) (*resampled_data)[i].open = (*resampled_data)[i].close;
        if ((*resampled_data)[i].high == 0) (*resampled_data)[i].high = (*resampled_data)[i].close;
        if ((*resampled_data)[i].low == 0)  (*resampled_data)[i].low = (*resampled_data)[i].close;
    }
   // Calculate pivot points
   int *pivot_values = malloc(*resampled_count * sizeof(int));
   optimize_pivot_calculations(*resampled_data, *resampled_count, pivot_values);
   
   // Apply pivot values and calculate pointpos
   for (int i = 0; i < *resampled_count; i++) {
       (*resampled_data)[i].pivot = pivot_values[i];
       (*resampled_data)[i].pointpos = pointpos((*resampled_data)[i]);
   }
   
   free(pivot_values);
   
   // Calculate EMAs
   double *prices = malloc(*resampled_count * sizeof(double));
   double *ema10 = malloc(*resampled_count * sizeof(double));
   double *ema20 = malloc(*resampled_count * sizeof(double));
   
   for (int i = 0; i < *resampled_count; i++) {
       prices[i] = (*resampled_data)[i].close;
   }
   
   calculate_ema(prices, *resampled_count, 10, ema10);
   calculate_ema(prices, *resampled_count, 20, ema20);
   
   for (int i = 0; i < *resampled_count; i++) {
       (*resampled_data)[i].ema10 = ema10[i];
       (*resampled_data)[i].ema20 = ema20[i];
   }
   
   free(prices);
   free(ema10);
   free(ema20);
}

/**
 * Update pivot values in Redis
 */
void update_pivot_values(const char *token_name, const char *token_value, redisContext *redis_context) {
    // Define lookback/forward constants
    const int lookback = 3;
    const int lookforward = 3;
    const int min_candles = lookback + 1 + lookforward;
    
    char pivot_key[100];
    sprintf(pivot_key, "%s:pivot_index:%s", RESAMPLED_PREFIX, token_name);
    
    // Get all candles from Redis for this token
    char index_key[100];
    sprintf(index_key, "%s:%s:index", RESAMPLED_PREFIX, token_value);
    
    // First check if we have a latest pivot index already stored
    redisReply *pivot_data_reply = redisCommand(redis_context, "HGETALL %s", pivot_key);
    
    int start_candle_index = 0;
    long long latest_known_timestamp = 0;
    
    // If we have pivot data, only process from that point forward
    if (pivot_data_reply && pivot_data_reply->type == REDIS_REPLY_ARRAY && pivot_data_reply->elements > 0) {
        for (size_t i = 0; i < pivot_data_reply->elements; i += 2) {
            if (strcmp(pivot_data_reply->element[i]->str, "pivot_index") == 0) {
                start_candle_index = atoi(pivot_data_reply->element[i+1]->str);
                
                // Go back lookback candles to ensure proper overlap for pivot calculation
                // but ensure we don't go negative
                start_candle_index = start_candle_index > lookback ? start_candle_index - lookback : 0;
            } else if (strcmp(pivot_data_reply->element[i]->str, "timestamp") == 0) {
                latest_known_timestamp = atoll(pivot_data_reply->element[i+1]->str);
            }
        }
    }
    
    if (pivot_data_reply) freeReplyObject(pivot_data_reply);
    
    // Get candle keys - if we know the latest timestamp, only get newer ones with some overlap
    redisReply *candles_reply;
    if (latest_known_timestamp > 0) {
        // Only get candles newer than the ones we've already processed
        // Include some overlap to ensure proper pivot calculation
        long long start_timestamp = latest_known_timestamp - (lookback * 5 * 60 * 1000); // Go back lookback candles
        
        candles_reply = redisCommand(redis_context, "ZRANGEBYSCORE %s %lld +inf", 
                                    index_key, start_timestamp);
    } else {
        candles_reply = redisCommand(redis_context, "ZRANGE %s 0 -1", index_key);
    }
    
    if (!candles_reply || candles_reply->type != REDIS_REPLY_ARRAY || candles_reply->elements < min_candles) {
        if (candles_reply) freeReplyObject(candles_reply);
        return;  // Not enough candles to perform update
    }
    
    // Use pipeline to get all candle data at once
    for (size_t i = 0; i < candles_reply->elements; i++) {
        redisAppendCommand(redis_context, "HGETALL %s", candles_reply->element[i]->str);
    }
    
    // Fetch all candle data
    MarketData *candle_data = malloc(candles_reply->elements * sizeof(MarketData));
    if (candle_data == NULL) {
        fprintf(stderr, "Failed to allocate memory for candle data\n");
        freeReplyObject(candles_reply);
        return;
    }
    int candle_count = 0;
    
    for (size_t i = 0; i < candles_reply->elements; i++) {
        redisReply *data_reply;
        if (redisGetReply(redis_context, (void**)&data_reply) != REDIS_OK) {
            continue;
        }
        
        if (data_reply && data_reply->type == REDIS_REPLY_ARRAY && data_reply->elements >= 2) {
            MarketData candle;
            memset(&candle, 0, sizeof(MarketData));
            strcpy(candle.symbol, token_name);
            
            for (size_t j = 0; j < data_reply->elements; j += 2) {
                char *field = data_reply->element[j]->str;
                char *value = data_reply->element[j+1]->str;
                
                if (strcmp(field, "timestamp") == 0) {
                    candle.timestamp = atoll(value);
                } else if (strcmp(field, "open") == 0) {
                    candle.open = atof(value);
                } else if (strcmp(field, "high") == 0) {
                    candle.high = atof(value);
                } else if (strcmp(field, "low") == 0) {
                    candle.low = atof(value);
                } else if (strcmp(field, "close") == 0) {
                    candle.close = atof(value);
                }
            }
            
            // Add to candle data array
            candle_data[candle_count++] = candle;
        }
        
        if (data_reply) freeReplyObject(data_reply);
    }
    
    // Skip processing if we don't have enough candles
    if (candle_count < min_candles) {
        free(candle_data);
        freeReplyObject(candles_reply);
        return;
    }
    
    // Sort candles by timestamp
    qsort(candle_data, candle_count, sizeof(MarketData), compare_market_data_by_timestamp);
    
    // Calculate pivot values (only for candles we need to update)
    int *pivot_values = malloc(candle_count * sizeof(int));
    if (pivot_values == NULL) {
        fprintf(stderr, "Failed to allocate memory for pivot values\n");
        free(candle_data);
        freeReplyObject(candles_reply);
        return;
    }
    optimize_pivot_calculations(candle_data, candle_count, pivot_values);
    
    // Track the latest pivot high
    int latest_pivot_index = -1;
    long long latest_pivot_timestamp = 0;
    
    // Create a pipeline for bulk Redis updates
    for (int i = 0; i < candle_count; i++) {
        char data_key[100];
        sprintf(data_key, "%s:%s:%lld", RESAMPLED_PREFIX, token_value, candle_data[i].timestamp);
        
        // If it's a pivot point, update both the pivot value and the pivot_candle_index
        if (pivot_values[i] > 0) {
            // Add HMSET to the pipeline
            redisAppendCommand(redis_context, "HMSET %s pivot %d pivot_candle_index %d", 
                            data_key, pivot_values[i], start_candle_index + i);
            
            // Track latest pivot high
            if (pivot_values[i] == 2 && 
                (latest_pivot_timestamp == 0 || candle_data[i].timestamp > latest_pivot_timestamp)) {
                latest_pivot_index = start_candle_index + i;
                latest_pivot_timestamp = candle_data[i].timestamp;
            }
        } else {
            // Just add pivot value update to pipeline
            redisAppendCommand(redis_context, "HSET %s pivot %d", data_key, pivot_values[i]);
        }
    }
    
    // Execute all the Redis commands in the pipeline
    for (int i = 0; i < candle_count; i++) {
        redisReply *reply;
        if (redisGetReply(redis_context, (void**)&reply) == REDIS_OK) {
            freeReplyObject(reply);
        }
    }
    
    // Update pivot tracking for the token
    if (latest_pivot_index >= 0) {
        long long current_time = (long long)time(NULL) * 1000;
        
        redisCommand(redis_context, "HMSET %s token_name %s pivot_index %d timestamp %lld last_updated %lld", 
                    pivot_key, token_name, latest_pivot_index, latest_pivot_timestamp, current_time);
    }
    
    free(pivot_values);
    free(candle_data);
    freeReplyObject(candles_reply);
}




/**
 * Store resampled data in Redis
 */
int store_resampled_data(MarketData *resampled_data, int count, const char *token_name, const char *token_value, redisContext *redis_context) {
    if (count == 0 || resampled_data == NULL) {
        return 0;
    }
    
    int stored_count = 0;
    
    // Prepare index key
    char index_key[100];
    sprintf(index_key, "%s:%s:index", RESAMPLED_PREFIX, token_value);
    
    // First, check which records already exist (in a pipeline)
    char **data_keys = malloc(count * sizeof(char*));
    int *exists_flags = calloc(count, sizeof(int));
    int valid_count = 0;
    
    // Step 1: Create all keys and queue EXISTS commands
    for (int i = 0; i < count; i++) {
        // Skip intervals that fall before 9:15 market open
        time_t candle_time = resampled_data[i].timestamp / 1000;
        struct tm *tm_candle = localtime(&candle_time);
        
        if (tm_candle->tm_hour == 9 && (tm_candle->tm_min == 5 || tm_candle->tm_min == 10)) {
            data_keys[i] = NULL;
            continue;
        }
        
        // Create hash key for this data point
        data_keys[i] = malloc(100);
        sprintf(data_keys[i], "%s:%s:%lld", RESAMPLED_PREFIX, token_value, resampled_data[i].timestamp);
        
        // Queue EXISTS command (don't execute yet)
        redisAppendCommand(redis_context, "EXISTS %s", data_keys[i]);
        valid_count++;
    }
    
    // Step 2: Get all EXISTS replies
    for (int i = 0; i < count; i++) {
        if (data_keys[i] != NULL) {
            redisReply *exists_reply;
            if (redisGetReply(redis_context, (void**)&exists_reply) == REDIS_OK) {
                exists_flags[i] = (exists_reply && exists_reply->integer == 1) ? 1 : 0;
                freeReplyObject(exists_reply);
            }
        }
    }
    
    // Step 3: Queue all HMSET commands
    for (int i = 0; i < count; i++) {
        if (data_keys[i] == NULL) {
            continue;
        }
        
        // Prepare pivot_candle_index value
        char pivot_index_str[20] = "";
        if (resampled_data[i].pivot > 0) {
            sprintf(pivot_index_str, "%d", i);
        }
        
        // Queue HMSET command (don't execute yet)
        redisAppendCommand(redis_context, 
            "HMSET %s symbol %s timestamp %lld open %f high %f low %f close %f adjVol %f ltp %f pivot %d pivot_candle_index %s ema10 %f ema20 %f",
            data_keys[i], 
            token_name,
            resampled_data[i].timestamp,
            resampled_data[i].open,
            resampled_data[i].high,
            resampled_data[i].low,
            resampled_data[i].close,
            resampled_data[i].volume,
            resampled_data[i].ltp,
            resampled_data[i].pivot,
            pivot_index_str,
            resampled_data[i].ema10,
            resampled_data[i].ema20
        );
        
        // Queue ZADD command if it's a new record
        if (!exists_flags[i]) {
            redisAppendCommand(redis_context, "ZADD %s %lld %s", 
                index_key, resampled_data[i].timestamp, data_keys[i]);
        }
        
        stored_count++;
    }
    
    // Step 4: Process all replies from HMSET commands
    for (int i = 0; i < count; i++) {
        if (data_keys[i] != NULL) {
            redisReply *hmset_reply;
            if (redisGetReply(redis_context, (void**)&hmset_reply) == REDIS_OK) {
                freeReplyObject(hmset_reply);
            }
            
            // Process ZADD reply if it was a new record
            if (!exists_flags[i]) {
                redisReply *zadd_reply;
                if (redisGetReply(redis_context, (void**)&zadd_reply) == REDIS_OK) {
                    freeReplyObject(zadd_reply);
                }
            }
        }
    }
    
    // Clean up
    for (int i = 0; i < count; i++) {
        if (data_keys[i] != NULL) {
            free(data_keys[i]);
        }
    }
    free(data_keys);
    free(exists_flags);
    
    return stored_count;
}

void* process_token_batch(void *arg) {
    ThreadData *thread_data = (ThreadData *)arg;
    TokenPair *tokens = thread_data->tokens;
    int token_count = thread_data->token_count;
    time_t since_timestamp = thread_data->last_run_time;
    
    // For timing measurements
    struct timeval token_start, token_end;
    double token_elapsed;
    
    // Connect to Redis for raw data source
    redisContext *resampled_redis = redis_connect(RESAMPLED_DB);
    if (resampled_redis == NULL) {
        printf("Failed to connect to Redis for resampled data\n");
        free(thread_data);
        return NULL;
    }
    
    // Process each token
    for (int i = 0; i < token_count; i++) {
        // Start timing for this token
        gettimeofday(&token_start, NULL);
        
        MarketData *raw_data = NULL;
        int raw_count = 0;
        
        // Get raw data for this token
        if (get_data_batch(&tokens[i], 1, since_timestamp, &raw_data, &raw_count) != 0) {
            printf("Error retrieving data for token %s\n", tokens[i].name);
            continue;
        }
        
        if (raw_count == 0) {
            // printf("No data found for token %s\n", tokens[i].name);
            free(raw_data);
            continue;
        }
        
        // Get the last known EMA values from the last candle in previous data
        double last_ema10 = 0.0;
        double last_ema20 = 0.0;
        
        // Find the most recent resampled candle for this token
        char index_key[100];
        sprintf(index_key, "%s:%s:index", RESAMPLED_PREFIX, tokens[i].value);
        
        // Get the latest candle key
        redisReply *last_candle_reply = redisCommand(resampled_redis, "ZRANGE %s -1 -1", index_key);
        if (last_candle_reply && last_candle_reply->elements > 0) {
            // Get the EMA values from this candle
            redisReply *ema_reply = redisCommand(resampled_redis, "HMGET %s ema10 ema20", 
                                                last_candle_reply->element[0]->str);
            if (ema_reply && ema_reply->elements == 2) {
                if (ema_reply->element[0]->type == REDIS_REPLY_STRING)
                    last_ema10 = atof(ema_reply->element[0]->str);
                if (ema_reply->element[1]->type == REDIS_REPLY_STRING)
                    last_ema20 = atof(ema_reply->element[1]->str);
            }
            if (ema_reply) freeReplyObject(ema_reply);
        }
        if (last_candle_reply) freeReplyObject(last_candle_reply);
        
        // Prepare and resample data
        MarketData *resampled_data = NULL;
        int resampled_count = 0;
        
        
        prepare_dataframe(raw_data, raw_count, &resampled_data, &resampled_count);
        

        if (resampled_count == 0) {
            printf("No resampled data for token %s\n", tokens[i].name);
            free(raw_data);
            free(resampled_data);
            continue;
        }
        
        // Override the EMA calculations with continuous values if we have last EMAs
        if (last_ema10 > 0.0 && last_ema20 > 0.0) {
            // Extract close prices
            double *prices = malloc(resampled_count * sizeof(double));
            double *ema10 = malloc(resampled_count * sizeof(double));
            double *ema20 = malloc(resampled_count * sizeof(double));
            
            for (int j = 0; j < resampled_count; j++) {
                prices[j] = resampled_data[j].close;
            }
            
            // Calculate EMAs with continuity
            calculate_ongoing_ema(prices, resampled_count, 10, last_ema10, ema10);
            calculate_ongoing_ema(prices, resampled_count, 20, last_ema20, ema20);
            
            // Update the values in the resampled data
            for (int j = 0; j < resampled_count; j++) {
                resampled_data[j].ema10 = ema10[j];
                resampled_data[j].ema20 = ema20[j];
            }
            
            free(prices);
            free(ema10);
            free(ema20);
        }
        
        // Time the storage operations
        struct timeval store_start, store_end;
        gettimeofday(&store_start, NULL);
        
        // Store resampled data
        int stored_count = store_resampled_data(resampled_data, resampled_count, 
                                             tokens[i].name, tokens[i].value, resampled_redis);
        
        gettimeofday(&store_end, NULL);
        double store_time = (store_end.tv_sec - store_start.tv_sec) + 
                           (store_end.tv_usec - store_start.tv_usec) / 1000000.0;
        
        // Time the pivot update operations
        struct timeval pivot_start, pivot_end;
        gettimeofday(&pivot_start, NULL);
        
        // Update pivot values
        update_pivot_values(tokens[i].name, tokens[i].value, resampled_redis);
        
        gettimeofday(&pivot_end, NULL);
        double pivot_time = (pivot_end.tv_sec - pivot_start.tv_sec) + 
                           (pivot_end.tv_usec - pivot_start.tv_usec) / 1000000.0;
        
        
        // Clean up
        free(raw_data);
        free(resampled_data);
    }
    
    redisFree(resampled_redis);
    free(thread_data);
    return NULL;
}

/**
 * Main resampling function
 */
void run_resampler() {
    printf("Starting Redis ticker data resampler - Incremental Mode\n");
    
    // Set up resampled database structure
    setup_resampled_db();
    
    // Load tokens
    TokenPair tokens[MAX_TOKEN_DICT_SIZE];
    int token_count = 0;
    load_tokens(tokens, &token_count);
    
    if (token_count == 0) {
        printf("No tokens loaded. Exiting.\n");
        return;
    }
    
    // Initialize last_run_time to current time minus 12 hours
    // time_t last_run_time = time(NULL) - 43200;  // 12 hours ago
    time_t last_run_time = time(NULL) - (7 * 24 * 60 * 60);
    
    printf("\nStarting Live Data Resampling\n");
    
    // Wait until market open time (9:15 AM)
    time_t now = time(NULL);
    struct tm *tm_now = localtime(&now);
    struct tm market_open_tm = *tm_now;
    market_open_tm.tm_hour = 9;
    market_open_tm.tm_min = 15;
    market_open_tm.tm_sec = 0;
    time_t market_open_time = mktime(&market_open_tm);
    
    if (now < market_open_time) {
        printf("Waiting for market open at 9:15 AM...\n");
        sleep(market_open_time - now);
    }
    struct timeval start_time, end_time;
    double elapsed_time;

    
    // Main resampling loop
    while (1) {
        // Check if market is still open
        // if (!is_market_open()) {
        //     printf("Market is closed. Exiting.\n");
        //     break;
        // }
        
        // Calculate next run time (aligned to 5-minute intervals)
        time_t next_run = get_next_run_time(RESAMPLE_INTERVAL);
        now = time(NULL);
        int wait_seconds = next_run - now;
        
        printf("Next resampling run scheduled at %s (%d seconds from now)\n",
               ctime(&next_run), wait_seconds);
        
        // Wait until next run time
        sleep(wait_seconds);
        gettimeofday(&start_time, NULL);
        
        now = time(NULL);
        printf("\nStarting incremental resampling run at %s\n", ctime(&now));
        
        // Batch tokens for parallel processing
        int batch_size = token_count / MAX_WORKERS;
        if (batch_size == 0) batch_size = 1;
        
        int num_batches = (token_count + batch_size - 1) / batch_size;
        if (num_batches > MAX_WORKERS) num_batches = MAX_WORKERS;
        
        pthread_t threads[MAX_WORKERS];
        
        // Create worker threads
        for (int i = 0; i < num_batches; i++) {
            int start_idx = i * batch_size;
            int end_idx = start_idx + batch_size;
            if (end_idx > token_count) end_idx = token_count;
            
            ThreadData *thread_data = malloc(sizeof(ThreadData));
            if (thread_data == NULL) {
                fprintf(stderr, "Failed to allocate memory for thread data\n");
                // Consider how to handle this in the main loop - perhaps retry or skip this batch
                continue;  // Skip this batch if we can't allocate memory
            }
            thread_data->tokens = &tokens[start_idx];
            thread_data->token_count = end_idx - start_idx;
            thread_data->last_run_time = last_run_time;
            
            pthread_create(&threads[i], NULL, process_token_batch, thread_data);
        }
        
        // Wait for all threads to complete
        for (int i = 0; i < num_batches; i++) {
            pthread_join(threads[i], NULL);
        }
        // End timing for this run
        gettimeofday(&end_time, NULL);
        elapsed_time = (end_time.tv_sec - start_time.tv_sec) + 
                    (end_time.tv_usec - start_time.tv_usec) / 1000000.0;
        
        printf("\nResampling run completed in %.2f seconds\n", elapsed_time);
        
        
        // Update last run time to the start of this run
        last_run_time = now;
        
        
    }
}

/**
 * Clear the resampled database
 */
void clear_resampled_database() {
    redisContext *redis = redis_connect(RESAMPLED_DB);
    if (redis == NULL) {
        return;
    }
    
    printf("Clearing the resampled database (DB index %d)\n", RESAMPLED_DB);
    redisReply *reply = redisCommand(redis, "FLUSHDB");
    
    if (reply && reply->type == REDIS_REPLY_STATUS && strcmp(reply->str, "OK") == 0) {
        printf("Successfully cleared resampled data (DB index %d)\n", RESAMPLED_DB);
    } else {
        printf("Error clearing database\n");
    }
    
    if (reply) freeReplyObject(reply);
    redisFree(redis);
}

/**
 * Print data for a specific token
 */
void print_token_data(const char *token_name, const char *token_value) {
    printf("\n========================================================================\n");
    printf("DATA FOR TOKEN: %s\n", token_name);
    printf("========================================================================\n");
    
    // Connect to both databases
    redisContext *source_redis = redis_connect(SOURCE_DB);
    redisContext *resampled_redis = redis_connect(RESAMPLED_DB);
    
    if (source_redis == NULL || resampled_redis == NULL) {
        if (source_redis) redisFree(source_redis);
        if (resampled_redis) redisFree(resampled_redis);
        return;
    }
    
    // Get resampled data
    printf("\nRESAMPLED DATA FROM RESAMPLED DB (%d):\n", RESAMPLED_DB);
    
    char resampled_index_key[100];
    sprintf(resampled_index_key, "%s:%s:index", RESAMPLED_PREFIX, token_value);
    
    redisReply *resampled_keys_reply = redisCommand(resampled_redis, "ZRANGE %s 0 -1", resampled_index_key);
    
    if (resampled_keys_reply && resampled_keys_reply->type == REDIS_REPLY_ARRAY) {
        // Print header
        printf("%-20s %-10s %-10s %-10s %-10s %-10s %-10s %-10s\n",
               "Timestamp", "Open", "High", "Low", "Close", "Volume", "Pivot", "EMA10");
        
        // Print each row
        for (size_t i = 0; i < resampled_keys_reply->elements; i++) {
            redisReply *data_reply = redisCommand(resampled_redis, "HGETALL %s", 
                                                resampled_keys_reply->element[i]->str);
            
            if (data_reply && data_reply->type == REDIS_REPLY_ARRAY) {
                // Create a temporary dictionary for easier field access
                char *timestamp_str = NULL;
                char *open_str = NULL;
                char *high_str = NULL;
                char *low_str = NULL;
                char *close_str = NULL;
                char *volume_str = NULL;
                char *pivot_str = NULL;
                char *ema10_str = NULL;
                
                for (size_t j = 0; j < data_reply->elements; j += 2) {
                    char *field = data_reply->element[j]->str;
                    char *value = data_reply->element[j+1]->str;
                    
                    if (strcmp(field, "timestamp") == 0) timestamp_str = value;
                    else if (strcmp(field, "open") == 0) open_str = value;
                    else if (strcmp(field, "high") == 0) high_str = value;
                    else if (strcmp(field, "low") == 0) low_str = value;
                    else if (strcmp(field, "close") == 0) close_str = value;
                    else if (strcmp(field, "adjVol") == 0) volume_str = value;
                    else if (strcmp(field, "pivot") == 0) pivot_str = value;
                    else if (strcmp(field, "ema10") == 0) ema10_str = value;
                }
                
                // Format timestamp for display
                char formatted_time[30] = "";
                if (timestamp_str) {
                    long long ts = atoll(timestamp_str);
                    time_t timestamp = ts / 1000;
                    struct tm *timeinfo = localtime(&timestamp);
                    strftime(formatted_time, sizeof(formatted_time), "%Y-%m-%d %H:%M", timeinfo);
                }
                
                // Print row
                printf("%-20s %-10s %-10s %-10s %-10s %-10s %-10s %-10s\n",
                       formatted_time, 
                       open_str ? open_str : "-",
                       high_str ? high_str : "-",
                       low_str ? low_str : "-",
                       close_str ? close_str : "-",
                       volume_str ? volume_str : "-",
                       pivot_str ? pivot_str : "-",
                       ema10_str ? ema10_str : "-");
            }
            
            if (data_reply) freeReplyObject(data_reply);
        }
    } else {
        printf("No resampled data found for %s\n", token_name);
    }
    
    if (resampled_keys_reply) freeReplyObject(resampled_keys_reply);
    
    // Get pivot data
    printf("\nPIVOT DATA:\n");
    
    char pivot_key[100];
    sprintf(pivot_key, "%s:pivot_index:%s", RESAMPLED_PREFIX, token_name);
    
    redisReply *pivot_reply = redisCommand(resampled_redis, "HGETALL %s", pivot_key);
    
    if (pivot_reply && pivot_reply->type == REDIS_REPLY_ARRAY && pivot_reply->elements > 0) {
        for (size_t i = 0; i < pivot_reply->elements; i += 2) {
            printf("  %s: %s\n", pivot_reply->element[i]->str, pivot_reply->element[i+1]->str);
        }
    } else {
        printf("No pivot data found for %s\n", token_name);
    }
    
    if (pivot_reply) freeReplyObject(pivot_reply);
    
    // Clean up
    redisFree(source_redis);
    redisFree(resampled_redis);
}

/**
 * Main function
 */
int main(int argc, char *argv[]) {
    // Set global prefixes
    strcpy(SOURCE_PREFIX, get_date_prefix());
    // strcpy(SOURCE_PREFIX, "2may2025");
    sprintf(RESAMPLED_PREFIX, "%sresampled", SOURCE_PREFIX);
    
    // Parse command-line arguments
    bool debug_mode = false;
    bool clear_db = false;
    
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--debug") == 0) {
            debug_mode = true;
        } else if (strcmp(argv[i], "--clear") == 0) {
            clear_db = true;
        }
    }
    
    // Process arguments
    if (clear_db) {
        clear_resampled_database();
        return 0;
    }
    
    if (debug_mode) {
        // Load tokens
        TokenPair tokens[MAX_TOKEN_DICT_SIZE];
        int token_count = 0;
        load_tokens(tokens, &token_count);
        
        if (token_count > 0) {
            printf("Debug mode: Processing token %s\n", tokens[0].name);
            print_token_data(tokens[0].name, tokens[0].value);
        } else {
            printf("No tokens available for debug run\n");
        }
    } else {
        // Run the main resampler
        run_resampler();
    }
    
    return 0;
}

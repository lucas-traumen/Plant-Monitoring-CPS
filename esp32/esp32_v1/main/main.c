/**
 * @file main.c
 * @brief ESP32 Firmware v2.2 — Rau Cai Mam Brassica juncea Monitor (Multi-WiFi Fallback)
 *
 * @details
 * - Sensors: DHT11 (Moving Average 5), BH1750, ADS1115 (4ch single-ended), DS3231 RTC
 * - Actuator: Pump relay (active HIGH, GPIO 26), Light relay (GPIO 27)
 * - Protocol: MQTT → BBB (topic: cps/greenhouse/sensors)
 * - Network: Tự động chuyển đổi giữa nhiều điểm truy cập WiFi (Multi-SSID) nếu rớt mạng.
 * - Resilience: Ring buffer 64 packets để lưu offline và tự động drain khi WiFi phục hồi.
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/event_groups.h"
#include "freertos/semphr.h"
#include "freertos/queue.h"

#include "esp_system.h"
#include "esp_wifi.h"
#include "esp_event.h"
#include "esp_log.h"
#include "esp_timer.h"
#include "nvs_flash.h"
#include "driver/i2c_master.h"
#include "driver/gpio.h"
#include "mqtt_client.h"
#include "i2cdev.h"

#include <ads111x.h>
#include "bh1750.h"
#include "esp32-dht11.h"
#include "ds3231.h"

/* ================================================================
   CẤU HÌNH HỆ THỐNG WIIFI DẠNG MẢNG (MULTI-WIFI)
   ================================================================ */

/**
 * @brief Cấu trúc lưu trữ thông tin mạng WiFi
 */
typedef struct {
    const char *ssid;
    const char *password;
} wifi_cred_t;

/**
 * @brief Danh sách các mạng WiFi dự phòng. 
 * Hệ thống sẽ thử kết nối tuần tự từ trên xuống dưới.
 */
static const wifi_cred_t WIFI_NETWORKS[] = {
    {"Phòng toàn trai đẹp", "aicungdeptrai<3"},
    {"LUCAS",      "12345678"},
    {"Truong Lung",    "12345678"}
};
#define WIFI_NETWORK_COUNT (sizeof(WIFI_NETWORKS) / sizeof(wifi_cred_t))
#define WIFI_MAX_RETRY       3      /**< Số lần thử tối đa cho mỗi mạng trước khi đổi mạng khác */

/* ================================================================
   CẤU HÌNH HỆ THỐNG KHÁC
   ================================================================ */

#define NODE_ID              "BRASSICA_JUNCEA_01"
#define PLANT_NAME           "Rau Cải Mầm (Brassica juncea)"
#define FW_VERSION           "2.2.0"

/* MQTT */
#define MQTT_BROKER_URI      "mqtt://192.168.2.15"
#define MQTT_PORT            1883
#define MQTT_KEEPALIVE_S     60
#define MQTT_QOS             1

#define TOPIC_SENSOR         "cps/greenhouse/sensors"
#define TOPIC_STATUS         "cps/greenhouse/status"
#define TOPIC_CMD_PUMP       "cps/greenhouse/cmd/pump"
#define TOPIC_CMD_LIGHT      "cps/greenhouse/cmd/light"
#define TOPIC_CMD_PHASE      "cps/greenhouse/cmd/phase"   /* BBB -> ESP32: payload "1" hoặc "2" */

/* GPIO & HW MUX */
#define I2C_MASTER_SDA       21
#define I2C_MASTER_SCL       22
#define I2C_MASTER_PORT      I2C_NUM_0

#define DHT11_GPIO           16
#define RELAY_PUMP_GPIO      26      /* Active HIGH */
#define RELAY_LIGHT_GPIO     27      /* Active HIGH */

/* ADS1115 Configuration */
#define ADS1115_I2C_ADDR     ADS111X_ADDR_GND   /* 0x48 */
#define SOIL_CH_COUNT        4
static const ads111x_mux_t SOIL_MUX[SOIL_CH_COUNT] = {
    ADS111X_MUX_0_GND,   /* AIN0 */
    ADS111X_MUX_1_GND,   /* AIN1 */
    ADS111X_MUX_2_GND,   /* AIN2 */
    ADS111X_MUX_3_GND,   /* AIN3 */
};

/* BH1750 */
#define BH1750_I2C_ADDR      BH1750_ADDR_LO     /* 0x23 */

/* Agronomic Parameters */
#define TEMP_IDEAL_MIN       18.0f
#define TEMP_IDEAL_MAX       24.0f
#define TEMP_GERM_IDEAL      22.0f
#define HUM_PHASE1_MIN       70.0f
#define HUM_PHASE1_MAX       85.0f
#define HUM_PHASE2_MIN       50.0f
#define HUM_PHASE2_MAX       65.0f
#define SOIL_IDEAL_MIN       55.0f
#define SOIL_IDEAL_MAX       80.0f
#define LIGHT_LEAK_THRESHOLD 5.0f    /* lux -- Phase 1 hộp kín, >5 lux = lọt sáng */
#define LIGHT_PHASE2_MIN     150.0f  /* lux -- Đèn LED tối thiểu Phase 2 */
#define LIGHT_PHASE2_IDEAL   220.0f  /* lux -- Đèn LED tối ưu */

/* Timings */
#define SENSOR_READ_MS       5000
#define MQTT_PUBLISH_MS      5000
#define LIGHT_CTRL_MS        1000   /* Chu kỳ kiểm tra lịch đèn theo RTC */

/* Local debug logs: hữu ích khi BBB/gateway chưa bật */
#define SENSOR_LOG_EVERY_N        1    /* 1 = log mỗi gói sensor, tương ứng ~5s */
#define LIGHT_LOG_HEARTBEAT_S     30   /* log trạng thái đèn mỗi 30s nếu không đổi */
#define OFFLINE_LOG_EVERY_N       5    /* khi MQTT mất, log buffer mỗi 5 packet */
#define DRAIN_MAX_PER_CYCLE       5    /* số packet offline đẩy lại mỗi chu kỳ publish */

/* Phase & Light Schedule */
#define PHASE_1_DARK         1
#define PHASE_2_LIGHT        2
#define DEFAULT_PHASE        PHASE_1_DARK
#define LIGHT_ON_HOUR_UTC7   6
#define LIGHT_OFF_HOUR_UTC7  20

/* ADS1115 Calibration */
#define SOIL_V_DRY           3.0f
#define SOIL_V_WET           1.1f

/* ================================================================
   BIẾN TOÀN CỤC & TẠO CẤU TRÚC DỮ LIỆU
   ================================================================ */

static const char *TAG = "SPROUT";

/* WiFi & MQTT States */
static EventGroupHandle_t s_wifi_eg;
#define WIFI_CONNECTED_BIT  BIT0
#define WIFI_FAIL_BIT       BIT1

static esp_mqtt_client_handle_t s_mqtt = NULL;
static bool s_mqtt_connected = false;

/* Biến phục vụ chức năng Multi-WiFi */
static int s_current_wifi_index = 0;
static int s_wifi_retry_count   = 0;

/* Các biến logic */
static int  s_step           = 0;
static int  s_current_phase  = DEFAULT_PHASE;   /* phase do BBB đồng bộ xuống */
static SemaphoreHandle_t s_sensor_mutex;
static i2c_dev_t         s_ads_dev = {0};
static i2c_dev_t         s_rtc_dev = {0};

/**
 * @brief Cấu trúc bản ghi chứa toàn bộ dữ liệu Snapshot từ cảm biến
 */
typedef struct {
    float temperature;
    float air_humidity;
    float soil_pct[SOIL_CH_COUNT];
    float lux;
    bool  pump_state;
    bool  light_state;
    int   phase;
    char  light_mode[16];      /* AUTO_RTC */
    char  light_reason[24];    /* PHASE1_DARK / PHASE2_SCHEDULE */
    bool  dht11_ok;
    bool  bh1750_ok;
    bool  ads1115_ok;
    bool  rtc_ok;
    char  iso_time[32];     /* ISO8601 từ DS3231 */
    int   wifi_rssi;
    long  uptime_s;
} sensor_data_t;

static sensor_data_t g_sensor = {0};

/* ================================================================
   RING BUFFER - Tránh mất dữ liệu (Offline Storage)
   ================================================================ */

#define RING_BUF_SIZE        64     /* Số packet tối đa lưu khi offline */
#define JSON_MAX_LEN         768

typedef struct {
    char  json[JSON_MAX_LEN];
    bool  valid;
} ring_entry_t;

typedef struct {
    ring_entry_t buf[RING_BUF_SIZE];
    int          head;    
    int          tail;    
    int          count;   
    SemaphoreHandle_t mutex;
} ring_buf_t;

static ring_buf_t s_ring = {0};

/**
 * @brief Khởi tạo Ring Buffer để lưu trữ JSON Payload khi mất mạng
 */
static void ring_init(void) {
    s_ring.head  = 0;
    s_ring.tail  = 0;
    s_ring.count = 0;
    s_ring.mutex = xSemaphoreCreateMutex();
}

/**
 * @brief Đẩy một MQTT payload vào bộ đệm
 * @param json Chuỗi JSON cần lưu
 * @return true nếu thành công, false nếu bộ đệm bận (mutex lock failed)
 */
static bool ring_push(const char *json) {
    if (!xSemaphoreTake(s_ring.mutex, pdMS_TO_TICKS(50))) return false;

    if (s_ring.count == RING_BUF_SIZE) {
        s_ring.tail = (s_ring.tail + 1) % RING_BUF_SIZE;
        s_ring.count--;
        ESP_LOGW("RING", "Buffer đầy — ghi đè packet cũ nhất. Gateway/MQTT broker có thể chưa bật");
    }

    strlcpy(s_ring.buf[s_ring.head].json, json, JSON_MAX_LEN);
    s_ring.buf[s_ring.head].valid = true;
    s_ring.head  = (s_ring.head + 1) % RING_BUF_SIZE;
    s_ring.count++;

    int count_after_push = s_ring.count;
    xSemaphoreGive(s_ring.mutex);

    if ((count_after_push % OFFLINE_LOG_EVERY_N) == 0 || count_after_push == 1 || count_after_push == RING_BUF_SIZE) {
        ESP_LOGW("RING", "Đang lưu offline: %d/%d packet", count_after_push, RING_BUF_SIZE);
    }
    return true;
}

static bool ring_peek(char *out_json) {
    if (!xSemaphoreTake(s_ring.mutex, pdMS_TO_TICKS(50))) return false;
    bool has = (s_ring.count > 0);
    if (has) strlcpy(out_json, s_ring.buf[s_ring.tail].json, JSON_MAX_LEN);
    xSemaphoreGive(s_ring.mutex);
    return has;
}

static void ring_pop(void) {
    if (!xSemaphoreTake(s_ring.mutex, pdMS_TO_TICKS(50))) return;
    if (s_ring.count > 0) {
        s_ring.buf[s_ring.tail].valid = false;
        s_ring.tail  = (s_ring.tail + 1) % RING_BUF_SIZE;
        s_ring.count--;
    }
    xSemaphoreGive(s_ring.mutex);
}

static int ring_count(void) {
    if (!xSemaphoreTake(s_ring.mutex, pdMS_TO_TICKS(50))) return 0;
    int c = s_ring.count;
    xSemaphoreGive(s_ring.mutex);
    return c;
}

/* ================================================================
   BỘ LỌC MOVING AVERAGE DHT11
   ================================================================ */

#define DHT_FILTER_SIZE 5

typedef struct {
    float temp_buf[DHT_FILTER_SIZE];
    float hum_buf[DHT_FILTER_SIZE];
    uint8_t index;
    uint8_t count;
} dht_filter_t;

static dht_filter_t s_dht_ma = {0};

static void dht11_moving_avg(float t, float h, float *out_t, float *out_h) {
    s_dht_ma.temp_buf[s_dht_ma.index] = t;
    s_dht_ma.hum_buf[s_dht_ma.index]  = h;
    s_dht_ma.index = (s_dht_ma.index + 1) % DHT_FILTER_SIZE;
    if (s_dht_ma.count < DHT_FILTER_SIZE) s_dht_ma.count++;

    float st = 0, sh = 0;
    for (int i = 0; i < s_dht_ma.count; i++) {
        st += s_dht_ma.temp_buf[i];
        sh += s_dht_ma.hum_buf[i];
    }
    *out_t = st / s_dht_ma.count;
    *out_h = sh / s_dht_ma.count;
}

/* ================================================================
   HELPER - RTC & CẢM BIẾN
   ================================================================ */

static void rtc_sync_compile_time(void) {
    struct tm t = {0};
    if (ds3231_get_time(&s_rtc_dev, &t) != ESP_OK) return;

    if (t.tm_year <= 100) {   
        struct tm ct = {0};
        strptime(__DATE__ " " __TIME__, "%b %d %Y %H:%M:%S", &ct);
        if (ds3231_set_time(&s_rtc_dev, &ct) == ESP_OK) {
            ESP_LOGW("RTC", "Đồng bộ RTC từ compile time: %s %s", __DATE__, __TIME__);
        }
    }
}

static void rtc_get_iso(char *buf, size_t len) {
    struct tm t = {0};
    if (ds3231_get_time(&s_rtc_dev, &t) == ESP_OK && t.tm_year > 100) {
        strftime(buf, len, "%Y-%m-%dT%H:%M:%S+07:00", &t);
    } else {
        long up = (long)(esp_timer_get_time() / 1000000);
        snprintf(buf, len, "uptime:%lds", up);
    }
}

static float ads_voltage_to_pct(double v) {
    if (v >= SOIL_V_DRY) return 0.0f;
    if (v <= SOIL_V_WET) return 100.0f;
    return (SOIL_V_DRY - (float)v) / (SOIL_V_DRY - SOIL_V_WET) * 100.0f;
}

static bool mqtt_topic_match(esp_mqtt_event_handle_t ev, const char *topic) {
    size_t topic_len = strlen(topic);
    return (ev->topic_len == topic_len) &&
           (strncmp(ev->topic, topic, topic_len) == 0);
}

static int parse_phase_payload(const char *data, int len) {
    /*
     * Hỗ trợ payload đơn giản:
     *   "1"
     *   "2"
     * hoặc JSON ngắn:
     *   {"phase":1}
     *   {"phase":2}
     */
    for (int i = 0; i < len; i++) {
        if (data[i] == '1') return PHASE_1_DARK;
        if (data[i] == '2') return PHASE_2_LIGHT;
    }
    return -1;
}

static bool rtc_get_hour_utc7(int *hour_out) {
    struct tm t = {0};
    if (ds3231_get_time(&s_rtc_dev, &t) == ESP_OK && t.tm_year > 100) {
        *hour_out = t.tm_hour;
        return true;
    }
    return false;
}

static bool light_should_be_on_by_schedule(int phase, int hour_utc7) {
    if (phase != PHASE_2_LIGHT) {
        return false;   /* Phase 1: luôn tối */
    }

    if (LIGHT_ON_HOUR_UTC7 < LIGHT_OFF_HOUR_UTC7) {
        return (hour_utc7 >= LIGHT_ON_HOUR_UTC7 &&
                hour_utc7 <  LIGHT_OFF_HOUR_UTC7);
    }

    /* Trường hợp lịch qua nửa đêm, ví dụ 20h -> 6h */
    return (hour_utc7 >= LIGHT_ON_HOUR_UTC7 ||
            hour_utc7 <  LIGHT_OFF_HOUR_UTC7);
}

static int build_json(char *buf, size_t buf_len, const sensor_data_t *s, int step) {
    float soil_avg = (s->soil_pct[0] + s->soil_pct[1] +
                      s->soil_pct[2] + s->soil_pct[3]) / 4.0f;
    return snprintf(buf, buf_len,
        "{"
        "\"node_id\":\"%s\","
        "\"fw\":\"%s\","
        "\"timestamp\":\"%s\","
        "\"step\":%d,"
        "\"uptime_s\":%ld,"
        "\"phase\":%d,"
        "\"sensor\":{"
            "\"temperature\":%.1f,"
            "\"air_humidity\":%.1f,"
            "\"lux\":%.2f,"
            "\"soil_moisture_avg\":%.1f,"
            "\"soil_moisture_raw\":{"
                "\"s1\":%.1f,\"s2\":%.1f,\"s3\":%.1f,\"s4\":%.1f"
            "}"
        "},"
        "\"status\":{"
            "\"wifi_rssi\":%d,"
            "\"dht11_ok\":%s,"
            "\"bh1750_ok\":%s,"
            "\"ads1115_ok\":%s,"
            "\"rtc_ok\":%s,"
            "\"pump_on\":%s,"
            "\"light_on\":%s,"
            "\"light_mode\":\"%s\","
            "\"light_reason\":\"%s\""
        "}"
        "}",
        NODE_ID, FW_VERSION, s->iso_time, step,
        s->uptime_s, s->phase,
        s->temperature, s->air_humidity, s->lux, soil_avg,
        s->soil_pct[0], s->soil_pct[1], s->soil_pct[2], s->soil_pct[3],
        s->wifi_rssi,
        s->dht11_ok   ? "true" : "false",
        s->bh1750_ok  ? "true" : "false",
        s->ads1115_ok ? "true" : "false",
        s->rtc_ok     ? "true" : "false",
        s->pump_state ? "true" : "false",
        s->light_state ? "true" : "false",
        s->light_mode[0] ? s->light_mode : "AUTO_RTC",
        s->light_reason[0] ? s->light_reason : "UNKNOWN"
    );
}

static const char *phase_to_text(int phase) {
    switch (phase) {
        case PHASE_1_DARK:  return "PHASE1_DARK";
        case PHASE_2_LIGHT: return "PHASE2_LIGHT";
        default:            return "UNKNOWN";
    }
}

static float soil_avg_from_snapshot(const sensor_data_t *s) {
    return (s->soil_pct[0] + s->soil_pct[1] +
            s->soil_pct[2] + s->soil_pct[3]) / 4.0f;
}

static void log_sensor_snapshot(const sensor_data_t *s, int step, int ring_used) {
    ESP_LOGI(TAG,
             "SENSOR step=%d phase=%d(%s) rtc=%s time=%s | "
             "T=%.1fC RH=%.1f%% Lux=%.2f Soil=%.1f%% "
             "[%.1f %.1f %.1f %.1f] | Pump=%s Light=%s/%s reason=%s | "
             "RSSI=%d MQTT=%s Buffer=%d/%d",
             step, s->phase, phase_to_text(s->phase),
             s->rtc_ok ? "OK" : "ERR", s->iso_time,
             s->temperature, s->air_humidity, s->lux, soil_avg_from_snapshot(s),
             s->soil_pct[0], s->soil_pct[1], s->soil_pct[2], s->soil_pct[3],
             s->pump_state ? "ON" : "OFF",
             s->light_state ? "ON" : "OFF",
             s->light_mode[0] ? s->light_mode : "AUTO_RTC",
             s->light_reason[0] ? s->light_reason : "UNKNOWN",
             s->wifi_rssi, s_mqtt_connected ? "ON" : "OFF",
             ring_used, RING_BUF_SIZE);
}

/* ================================================================
   WIFI EVENT HANDLER DÀNH CHO MULTI-WIFI
   ================================================================ */

/**
 * @brief Chuyển đổi sang điểm truy cập WiFi tiếp theo trong danh sách
 */
static void wifi_switch_to_next_network(void) {
    s_current_wifi_index = (s_current_wifi_index + 1) % WIFI_NETWORK_COUNT;
    s_wifi_retry_count = 0;

    ESP_LOGI(TAG, "==> Đổi sang WiFi: %s", WIFI_NETWORKS[s_current_wifi_index].ssid);

    wifi_config_t wifi_config = {0};
    strlcpy((char *)wifi_config.sta.ssid, WIFI_NETWORKS[s_current_wifi_index].ssid, sizeof(wifi_config.sta.ssid));
    strlcpy((char *)wifi_config.sta.password, WIFI_NETWORKS[s_current_wifi_index].password, sizeof(wifi_config.sta.password));
    wifi_config.sta.threshold.authmode = WIFI_AUTH_WPA2_PSK;

    ESP_ERROR_CHECK(esp_wifi_set_config(WIFI_IF_STA, &wifi_config));
    esp_wifi_connect();
}

static void wifi_event_handler(void *arg, esp_event_base_t base,
                                int32_t id, void *data) {
    if (base == WIFI_EVENT && id == WIFI_EVENT_STA_START) {
        esp_wifi_connect();
    } else if (base == WIFI_EVENT && id == WIFI_EVENT_STA_DISCONNECTED) {
        s_mqtt_connected = false;
        
        if (s_wifi_retry_count < WIFI_MAX_RETRY) {
            s_wifi_retry_count++;
            ESP_LOGW(TAG, "WiFi rớt. Đang thử lại %d/%d (SSID: %s)", 
                     s_wifi_retry_count, WIFI_MAX_RETRY, 
                     WIFI_NETWORKS[s_current_wifi_index].ssid);
            esp_wifi_connect();
        } else {
            ESP_LOGE(TAG, "Kết nối [%s] thất bại hoàn toàn. Tiến hành quét chuyển mạng...", 
                     WIFI_NETWORKS[s_current_wifi_index].ssid);
                     
            /* Đổi sang mạng kế tiếp */
            wifi_switch_to_next_network();

            /* Nếu đã duyệt hết 1 vòng mảng WiFi mà vẫn rớt -> báo FAIL để app_main không bị kẹt khi boot (chạy offline) */
            if (s_current_wifi_index == 0) {
                xEventGroupSetBits(s_wifi_eg, WIFI_FAIL_BIT);
            }
        }
    } else if (base == IP_EVENT && id == IP_EVENT_STA_GOT_IP) {
        ip_event_got_ip_t *event = (ip_event_got_ip_t *) data;
        ESP_LOGI(TAG, "Thành công lấy IP: " IPSTR " (SSID: %s)", 
                 IP2STR(&event->ip_info.ip), WIFI_NETWORKS[s_current_wifi_index].ssid);
        s_wifi_retry_count = 0;
        xEventGroupSetBits(s_wifi_eg, WIFI_CONNECTED_BIT);
    }
}

static void wifi_init(void) {
    s_wifi_eg = xEventGroupCreate();
    ESP_ERROR_CHECK(esp_netif_init());
    ESP_ERROR_CHECK(esp_event_loop_create_default());
    esp_netif_create_default_wifi_sta();

    wifi_init_config_t cfg = WIFI_INIT_CONFIG_DEFAULT();
    ESP_ERROR_CHECK(esp_wifi_init(&cfg));

    esp_event_handler_instance_register(WIFI_EVENT, ESP_EVENT_ANY_ID,
                                        wifi_event_handler, NULL, NULL);
    esp_event_handler_instance_register(IP_EVENT, IP_EVENT_STA_GOT_IP,
                                        wifi_event_handler, NULL, NULL);

    /* Cấu hình mạng WiFi đầu tiên trong mảng */
    wifi_config_t wcfg = {0};
    strlcpy((char *)wcfg.sta.ssid, WIFI_NETWORKS[s_current_wifi_index].ssid, sizeof(wcfg.sta.ssid));
    strlcpy((char *)wcfg.sta.password, WIFI_NETWORKS[s_current_wifi_index].password, sizeof(wcfg.sta.password));
    wcfg.sta.threshold.authmode = WIFI_AUTH_WPA2_PSK;

    ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_STA));
    ESP_ERROR_CHECK(esp_wifi_set_config(WIFI_IF_STA, &wcfg));
    ESP_ERROR_CHECK(esp_wifi_start());

    /* Đợi kết nối hoặc lỗi (Duyệt xong 1 vòng mảng) */
    EventBits_t bits = xEventGroupWaitBits(s_wifi_eg,
        WIFI_CONNECTED_BIT | WIFI_FAIL_BIT, pdFALSE, pdFALSE,
        pdMS_TO_TICKS(15000));

    if (bits & WIFI_CONNECTED_BIT) {
        ESP_LOGI(TAG, "Quá trình Boot WiFi Hoàn Tất");
    } else {
        ESP_LOGE(TAG, "Không tìm thấy mạng khả dụng — chuyển sang chế độ Offline (Buffering active)");
    }
}

/* ================================================================
   MQTT EVENT HANDLER
   ================================================================ */

static void mqtt_event_handler(void *arg, esp_event_base_t base, int32_t id, void *data) {
    esp_mqtt_event_handle_t ev = (esp_mqtt_event_handle_t)data;
    switch ((esp_mqtt_event_id_t)id) {
        case MQTT_EVENT_CONNECTED:
            s_mqtt_connected = true;
            esp_mqtt_client_subscribe(s_mqtt, TOPIC_CMD_PUMP,  MQTT_QOS);
            esp_mqtt_client_subscribe(s_mqtt, TOPIC_CMD_PHASE, MQTT_QOS);
            ESP_LOGI(TAG, "MQTT kết nối %s:%d — subscribed cmd/pump + cmd/phase | buffer=%d/%d",
                     MQTT_BROKER_URI, MQTT_PORT, ring_count(), RING_BUF_SIZE);
            break;
        case MQTT_EVENT_DISCONNECTED:
            s_mqtt_connected = false;
            ESP_LOGW(TAG, "MQTT mất kết nối — buffering ON | broker=%s:%d | buffer=%d/%d",
                     MQTT_BROKER_URI, MQTT_PORT, ring_count(), RING_BUF_SIZE);
            break;
        case MQTT_EVENT_ERROR:
            s_mqtt_connected = false;
            ESP_LOGE(TAG, "MQTT_EVENT_ERROR — chưa kết nối được broker/gateway? broker=%s:%d | buffer=%d/%d",
                     MQTT_BROKER_URI, MQTT_PORT, ring_count(), RING_BUF_SIZE);
            break;
        case MQTT_EVENT_DATA:
            if (mqtt_topic_match(ev, TOPIC_CMD_PUMP)) {
                bool on = (strncmp(ev->data, "ON", ev->data_len) == 0);
                gpio_set_level(RELAY_PUMP_GPIO, on ? 1 : 0);

                if (xSemaphoreTake(s_sensor_mutex, pdMS_TO_TICKS(100)) == pdTRUE) {
                    g_sensor.pump_state = on;
                    xSemaphoreGive(s_sensor_mutex);
                }

                ESP_LOGI(TAG, "Command: Pump → %s", on ? "ON" : "OFF");
            } else if (mqtt_topic_match(ev, TOPIC_CMD_PHASE)) {
                int new_phase = parse_phase_payload(ev->data, ev->data_len);

                if (new_phase == PHASE_1_DARK || new_phase == PHASE_2_LIGHT) {
                    s_current_phase = new_phase;

                    if (xSemaphoreTake(s_sensor_mutex, pdMS_TO_TICKS(100)) == pdTRUE) {
                        g_sensor.phase = new_phase;
                        xSemaphoreGive(s_sensor_mutex);
                    }

                    ESP_LOGI(TAG, "Command: Phase → %d (%s). ESP32 sẽ tự điều khiển đèn bằng RTC",
                             new_phase, phase_to_text(new_phase));
                } else {
                    ESP_LOGW(TAG, "Phase payload không hợp lệ: %.*s",
                             ev->data_len, ev->data);
                }
            }
            break;
        default: break;
    }
}

static void mqtt_init(void) {
    esp_mqtt_client_config_t cfg = {
        .broker.address.uri  = MQTT_BROKER_URI,
        .broker.address.port = MQTT_PORT,
        .session.keepalive   = MQTT_KEEPALIVE_S,
    };
    s_mqtt = esp_mqtt_client_init(&cfg);
    esp_mqtt_client_register_event(s_mqtt, ESP_EVENT_ANY_ID, mqtt_event_handler, NULL);
    esp_mqtt_client_start(s_mqtt);
}

/* ================================================================
   HARDWARE INIT
   ================================================================ */

static void hw_init(void) {
    ESP_ERROR_CHECK(i2cdev_init());

    memset(&s_rtc_dev, 0, sizeof(s_rtc_dev));
    if (ds3231_init_desc(&s_rtc_dev, I2C_MASTER_PORT,
                         I2C_MASTER_SDA, I2C_MASTER_SCL) != ESP_OK) {
        ESP_LOGE(TAG, "Lỗi: DS3231 init thất bại");
    } else {
        rtc_sync_compile_time();
    }

    gpio_config_t relay_cfg = {
        .pin_bit_mask = (1ULL << RELAY_PUMP_GPIO) | (1ULL << RELAY_LIGHT_GPIO),
        .mode         = GPIO_MODE_OUTPUT,
        .pull_down_en = GPIO_PULLDOWN_ENABLE,
        .pull_up_en   = GPIO_PULLUP_DISABLE,
        .intr_type    = GPIO_INTR_DISABLE,
    };
    gpio_config(&relay_cfg);
    gpio_set_level(RELAY_PUMP_GPIO,  0);
    gpio_set_level(RELAY_LIGHT_GPIO, 0);

    if (xSemaphoreTake(s_sensor_mutex, pdMS_TO_TICKS(100)) == pdTRUE) {
        g_sensor.phase = DEFAULT_PHASE;
        strlcpy(g_sensor.light_mode, "AUTO_RTC", sizeof(g_sensor.light_mode));
        strlcpy(g_sensor.light_reason, "PHASE1_DARK", sizeof(g_sensor.light_reason));
        xSemaphoreGive(s_sensor_mutex);
    }

    ESP_LOGI(TAG, "HW init xong | relay pump=GPIO%d OFF | relay light=GPIO%d OFF | default phase=%d(%s)",
             RELAY_PUMP_GPIO, RELAY_LIGHT_GPIO, DEFAULT_PHASE, phase_to_text(DEFAULT_PHASE));
}

/* ================================================================
   TASKS CỐT LÕI
   ================================================================ */

/**
 * @brief Task: Đọc tuần hoàn dữ liệu từ toàn bộ mảng cảm biến.
 */
static void sensor_task(void *pv) {
    dht11_t dht = { .dht11_pin = DHT11_GPIO };
    i2c_dev_t bh_dev = {0};
    bool bh_ready = false, ads_ready = false;

    if (bh1750_init_desc(&bh_dev, BH1750_I2C_ADDR, I2C_MASTER_PORT, I2C_MASTER_SDA, I2C_MASTER_SCL) == ESP_OK
        && bh1750_power_on(&bh_dev) == ESP_OK
        && bh1750_setup(&bh_dev, BH1750_MODE_CONTINUOUS, BH1750_RES_HIGH2) == ESP_OK) {
        bh_ready = true;
    }

    if (ads111x_init_desc(&s_ads_dev, ADS1115_I2C_ADDR, I2C_MASTER_PORT, I2C_MASTER_SDA, I2C_MASTER_SCL) == ESP_OK
        && ads111x_set_mode(&s_ads_dev, ADS111X_MODE_SINGLE_SHOT) == ESP_OK
        && ads111x_set_data_rate(&s_ads_dev, ADS111X_DATA_RATE_8) == ESP_OK
        && ads111x_set_gain(&s_ads_dev, ADS111X_GAIN_4V096) == ESP_OK) {
        ads_ready = true;
    }

    vTaskDelay(pdMS_TO_TICKS(2000));

    while (1) {
        sensor_data_t snap = {0};
        snap.uptime_s = (long)(esp_timer_get_time() / 1000000LL);
        rtc_get_iso(snap.iso_time, sizeof(snap.iso_time));
        snap.rtc_ok = (snap.iso_time[0] == '2');

        if (dht11_read(&dht, 3) == 0) {
            float ft, fh;
            dht11_moving_avg(dht.temperature, dht.humidity, &ft, &fh);
            snap.temperature = ft; snap.air_humidity = fh; snap.dht11_ok = true;
        } else {
            if (xSemaphoreTake(s_sensor_mutex, pdMS_TO_TICKS(100)) == pdTRUE) {
                snap.temperature = g_sensor.temperature; snap.air_humidity = g_sensor.air_humidity;
                xSemaphoreGive(s_sensor_mutex);
            }
        }

        if (bh_ready) {
            uint16_t raw = 0;
            if (bh1750_read(&bh_dev, &raw) == ESP_OK) {
                snap.lux = (float)raw / 2.0f; snap.bh1750_ok = true;
            }
        }

        if (ads_ready) {
            bool all_ok = true;
            for (int i = 0; i < SOIL_CH_COUNT; i++) {
                if (ads111x_set_input_mux(&s_ads_dev, SOIL_MUX[i]) != ESP_OK) { all_ok = false; continue; }
                if (ads111x_start_conversion(&s_ads_dev) != ESP_OK) { all_ok = false; continue; }
                vTaskDelay(pdMS_TO_TICKS(150));
                
                bool busy = true;
                for (int w = 0; w < 8 && busy; w++) {
                    ads111x_is_busy(&s_ads_dev, &busy);
                    if (busy) vTaskDelay(pdMS_TO_TICKS(25));
                }
                int16_t raw_v = 0;
                if (ads111x_get_value(&s_ads_dev, &raw_v) == ESP_OK) {
                    double voltage = (double)raw_v * 4.096 / 32767.0;
                    snap.soil_pct[i] = ads_voltage_to_pct(voltage);
                } else { all_ok = false; }
            }
            snap.ads1115_ok = all_ok;
        }

        wifi_ap_record_t ap;
        if (esp_wifi_sta_get_ap_info(&ap) == ESP_OK) snap.wifi_rssi = ap.rssi;

        if (xSemaphoreTake(s_sensor_mutex, pdMS_TO_TICKS(100)) == pdTRUE) {
            snap.pump_state = g_sensor.pump_state;
            snap.light_state = g_sensor.light_state;
            snap.phase       = g_sensor.phase ? g_sensor.phase : DEFAULT_PHASE;
            strlcpy(snap.light_mode,
                    g_sensor.light_mode[0] ? g_sensor.light_mode : "AUTO_RTC",
                    sizeof(snap.light_mode));
            strlcpy(snap.light_reason,
                    g_sensor.light_reason[0] ? g_sensor.light_reason : "UNKNOWN",
                    sizeof(snap.light_reason));
            g_sensor = snap;
            xSemaphoreGive(s_sensor_mutex);
        }

        static int sensor_log_tick = 0;
        sensor_log_tick++;
        if ((sensor_log_tick % SENSOR_LOG_EVERY_N) == 0) {
            log_sensor_snapshot(&snap, s_step, ring_count());
        }

        vTaskDelay(pdMS_TO_TICKS(SENSOR_READ_MS));
    }
}

/**
 * @brief Task: Điều khiển đèn theo phase đã được BBB đồng bộ và RTC DS3231.
 *
 * Luồng hiện tại:
 * - BBB gửi phase xuống topic cps/greenhouse/cmd/phase
 * - ESP32 tự dùng RTC để bật/tắt đèn:
 *   + Phase 1: luôn OFF
 *   + Phase 2: ON trong khung LIGHT_ON_HOUR_UTC7 -> LIGHT_OFF_HOUR_UTC7
 * - Chưa xử lý fallback mất mạng ở đây.
 */
static void light_schedule_task(void *pv) {
    bool last_light_on = false;
    int  last_phase = -1;
    char last_reason[24] = {0};
    int heartbeat_tick = 0;

    while (1) {
        int phase = s_current_phase;
        int hour_utc7 = -1;
        bool rtc_ok = rtc_get_hour_utc7(&hour_utc7);
        bool light_on = false;
        const char *reason = "RTC_ERROR";

        if (rtc_ok) {
            light_on = light_should_be_on_by_schedule(phase, hour_utc7);
            reason = (phase == PHASE_1_DARK) ? "PHASE1_DARK" :
                     (light_on ? "PHASE2_SCHEDULE_ON" : "PHASE2_SCHEDULE_OFF");
        }

        gpio_set_level(RELAY_LIGHT_GPIO, light_on ? 1 : 0);

        if (xSemaphoreTake(s_sensor_mutex, pdMS_TO_TICKS(100)) == pdTRUE) {
            g_sensor.phase = phase;
            g_sensor.light_state = light_on;
            g_sensor.rtc_ok = rtc_ok;
            strlcpy(g_sensor.light_mode, "AUTO_RTC", sizeof(g_sensor.light_mode));
            strlcpy(g_sensor.light_reason, reason, sizeof(g_sensor.light_reason));
            xSemaphoreGive(s_sensor_mutex);
        }

        heartbeat_tick++;
        bool changed = (phase != last_phase) ||
                       (light_on != last_light_on) ||
                       (strncmp(reason, last_reason, sizeof(last_reason)) != 0);

        if (changed || heartbeat_tick >= (LIGHT_LOG_HEARTBEAT_S * 1000 / LIGHT_CTRL_MS)) {
            ESP_LOGI(TAG, "LIGHT phase=%d(%s) rtc=%s hour=%d schedule=%02d-%02d => light=%s reason=%s",
                     phase, phase_to_text(phase), rtc_ok ? "OK" : "ERR", hour_utc7,
                     LIGHT_ON_HOUR_UTC7, LIGHT_OFF_HOUR_UTC7,
                     light_on ? "ON" : "OFF", reason);
            last_phase = phase;
            last_light_on = light_on;
            strlcpy(last_reason, reason, sizeof(last_reason));
            heartbeat_tick = 0;
        }

        vTaskDelay(pdMS_TO_TICKS(LIGHT_CTRL_MS));
    }
}

/**
 * @brief Task: Xuất dữ liệu lên MQTT và giải phóng bộ đệm (Drain Buffer).
 */
static void publish_task(void *pv) {
    static char json_buf[JSON_MAX_LEN];
    static char drain_buf[JSON_MAX_LEN];

    while (1) {
        vTaskDelay(pdMS_TO_TICKS(MQTT_PUBLISH_MS));
        sensor_data_t snap;
        if (xSemaphoreTake(s_sensor_mutex, pdMS_TO_TICKS(200)) == pdTRUE) {
            snap = g_sensor;
            xSemaphoreGive(s_sensor_mutex);
        } else continue;

        s_step++;
        int len = build_json(json_buf, sizeof(json_buf), &snap, s_step);
        if (len <= 0) continue;

        int ring_before = ring_count();
        if (s_mqtt_connected) {
            int msg_id = esp_mqtt_client_publish(s_mqtt, TOPIC_SENSOR, json_buf, len, MQTT_QOS, 0);
            if (msg_id < 0) {
                ESP_LOGW(TAG, "Publish fail step=%d — lưu buffer", s_step);
                ring_push(json_buf);
            } else {
                ESP_LOGI(TAG, "PUBLISH step=%d msg_id=%d len=%d phase=%d soil=%.1f%% light=%s buffer=%d/%d",
                         s_step, msg_id, len, snap.phase, soil_avg_from_snapshot(&snap),
                         snap.light_state ? "ON" : "OFF", ring_before, RING_BUF_SIZE);
            }

            int drained = 0;
            while (s_mqtt_connected && ring_count() > 0 && drained < DRAIN_MAX_PER_CYCLE) {
                if (!ring_peek(drain_buf)) break;
                if (esp_mqtt_client_publish(s_mqtt, TOPIC_SENSOR, drain_buf, strlen(drain_buf), MQTT_QOS, 0) >= 0) {
                    ring_pop();
                    drained++;
                } else break;
                vTaskDelay(pdMS_TO_TICKS(200));
            }
            if (drained > 0) {
                ESP_LOGI(TAG, "DRAIN buffer: đã gửi lại %d packet, còn %d/%d",
                         drained, ring_count(), RING_BUF_SIZE);
            }
        } else {
            ring_push(json_buf);
            if ((s_step % OFFLINE_LOG_EVERY_N) == 0 || ring_count() == 1) {
                ESP_LOGW(TAG, "OFFLINE step=%d — gateway/MQTT chưa sẵn sàng, lưu packet vào ring buffer %d/%d",
                         s_step, ring_count(), RING_BUF_SIZE);
            }
        }
    }
}

/* ================================================================
   MAIN ENTRY
   ================================================================ */

void app_main(void) {
    ESP_LOGI(TAG, "=== %s | %s v%s ===", PLANT_NAME, NODE_ID, FW_VERSION);
    ESP_LOGI(TAG, "MQTT broker=%s:%d | sensor=%s | cmd_pump=%s | cmd_phase=%s",
             MQTT_BROKER_URI, MQTT_PORT, TOPIC_SENSOR, TOPIC_CMD_PUMP, TOPIC_CMD_PHASE);

    esp_err_t nvs_ret = nvs_flash_init();
    if (nvs_ret == ESP_ERR_NVS_NO_FREE_PAGES || nvs_ret == ESP_ERR_NVS_NEW_VERSION_FOUND) {
        nvs_flash_erase();
        nvs_flash_init();
    }

    s_sensor_mutex = xSemaphoreCreateMutex();
    ring_init();
    hw_init();
    wifi_init();
    mqtt_init();

    xTaskCreate(sensor_task,         "sensor",  4096, NULL, 5, NULL);
    xTaskCreate(light_schedule_task, "light",   3072, NULL, 4, NULL);
    xTaskCreate(publish_task,        "publish", 4096, NULL, 3, NULL);

    ESP_LOGI(TAG, "Tất cả tasks đã khởi động (Chế độ Multi-WiFi khả dụng)");
}

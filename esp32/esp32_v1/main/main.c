/**
 * @file main.c
 * @brief ESP32 Firmware v2.9.3 — Sensor Fusion + Planting Start NVS Sync
 *
 * @details
 * - Sensors: DHT11, BH1750, ADS1115 4-point soil array, DS3231 RTC
 * - Actuator: Pump relay (active HIGH, GPIO 19), Light relay (GPIO 18)
 * - Protocol: MQTT → Raspberry Pi broker (topic: cps/greenhouse/brassica_01/telemetry/sensors)
 * - Network: Tự động chuyển đổi giữa nhiều điểm truy cập WiFi (Multi-SSID) nếu rớt mạng.
 * - Filtering: median samples -> EMA -> stuck/outlier check -> Schmid-Schossmaier FTI fusion.
 * - Planting sync: ESP32 NVS is local cache; Gateway/DB is the source of truth.
 * - Phase owner: ESP32 đọc DS3231 RTC, tự tính Phase 1/2 theo planting_start_time,
 *   tự điều khiển đèn theo phase và gửi phase lên Raspberry Pi Gateway.
 * - Time sync: WiFi có mạng -> NTP -> cập nhật DS3231 nếu lệch > 5 giây.
 */

#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <stdlib.h>
#include <time.h>
#include <sys/time.h>

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/event_groups.h"
#include "freertos/semphr.h"
#include "freertos/queue.h"

#include "esp_system.h"
#include "esp_wifi.h"
#include "esp_event.h"
#include "esp_netif.h"
#include "esp_sntp.h"
#include "esp_log.h"
#include "esp_timer.h"
#include "nvs_flash.h"
#include "nvs.h"
#include "driver/i2c_master.h"
#include "driver/gpio.h"
#include "mqtt_client.h"
#include "i2cdev.h"

#include <ads111x.h>
#include "bh1750.h"
#include "esp32-dht11.h"
#include "ds3231.h"

/* Compatibility: một số ESP-IDF dùng SNTP_OPMODE_POLL thay vì ESP_SNTP_OPMODE_POLL. */
#ifndef ESP_SNTP_OPMODE_POLL
#define ESP_SNTP_OPMODE_POLL SNTP_OPMODE_POLL
#endif

/* ================================================================
   CẤU HÌNH WIFI — ESP32 KẾT NỐI RASPBERRY PI HOTSPOT
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
 * ESP32 kết nối vào hotspot Plant-Gateway do Raspberry Pi phát.
 */
static const wifi_cred_t WIFI_NETWORKS[] = {
    /* Raspberry Pi hotspot: wlan0=192.168.4.1, Mosquitto=192.168.4.1:1883 */
    {"Plant-Gateway", "Plant123456"}
};
#define WIFI_NETWORK_COUNT (sizeof(WIFI_NETWORKS) / sizeof(wifi_cred_t))
#define WIFI_MAX_RETRY       3      /**< Số lần thử tối đa cho mỗi mạng trước khi đổi mạng khác */
#define WIFI_SCAN_MAX_AP     50     /**< Số AP tối đa lưu khi scan; tăng để tránh “quét thiếu mạng” */
#define WIFI_BOOT_WAIT_MS    30000  /**< Thời gian chờ WiFi lúc boot; không quá ngắn để tránh offline giả */
#define WIFI_SCAN_MIN_MS     250    /**< Active scan min time per channel */
#define WIFI_SCAN_MAX_MS     900    /**< Active scan max time per channel */

/* ================================================================
   CẤU HÌNH HỆ THỐNG KHÁC
   ================================================================ */

#define NODE_ID              "BRASSICA_JUNCEA_01"
#define PLANT_NAME           "Rau Cải Mầm (Brassica juncea)"
#define FW_VERSION           "2.9.4-direct-command-ack"

/* MQTT */
#define MQTT_BROKER_URI      "mqtt://192.168.4.1"   /* Raspberry Pi hotspot broker */
#define MQTT_PORT            1883
#define MQTT_KEEPALIVE_S     60
#define MQTT_QOS             1
#define MQTT_CLIENT_ID       "esp32_brassica_01"

/* MQTT topic tree v2: cps/greenhouse/<node>/... */
#define TOPIC_ROOT           "cps/greenhouse"
#define NODE_TOPIC_ID        "brassica_01"

/* ESP32 -> Broker */
#define TOPIC_SENSOR \
    TOPIC_ROOT "/" NODE_TOPIC_ID "/telemetry/sensors"

#define TOPIC_STATUS_ESP32 \
    TOPIC_ROOT "/" NODE_TOPIC_ID "/status/esp32"

#define TOPIC_ACTUATOR_STATE \
    TOPIC_ROOT "/" NODE_TOPIC_ID "/state/actuator"

/* Gateway -> Broker */
#define TOPIC_STATUS_GATEWAY \
    TOPIC_ROOT "/gateway/status"

/* Raspberry Pi Gateway AI AUTO -> ESP32 */
#define TOPIC_CMD_PUMP_AUTO \
    TOPIC_ROOT "/" NODE_TOPIC_ID "/cmd/auto/pump"

/* Unity/Web/Digital Twin DIRECT -> ESP32 */
#define TOPIC_CMD_PUMP_DIRECT \
    TOPIC_ROOT "/" NODE_TOPIC_ID "/cmd/direct/pump"

#define TOPIC_CMD_LIGHT_DIRECT \
    TOPIC_ROOT "/" NODE_TOPIC_ID "/cmd/direct/light"

/* Config command */
#define TOPIC_CMD_PLANTING_START \
    TOPIC_ROOT "/" NODE_TOPIC_ID "/cmd/config/planting_start"

/* GPIO & HW MUX */
#define I2C_MASTER_SDA       21
#define I2C_MASTER_SCL       22
#define I2C_MASTER_PORT      I2C_NUM_0

#define DHT11_GPIO           16
#define RELAY_PUMP_GPIO      19      /* Active HIGH */
#define RELAY_LIGHT_GPIO     18      /* Active HIGH */

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
#define LIGHT_LEAK_THRESHOLD 150.0f  /* lux -- Phase 1 hộp xốp, không tối tuyệt đối */
#define LIGHT_PHASE2_MIN     150.0f  /* lux -- Đèn LED tối thiểu Phase 2 */
#define LIGHT_PHASE2_IDEAL   220.0f  /* lux -- Đèn LED tối ưu */

/* Timings */
#define SENSOR_READ_MS       1000
#define MQTT_PUBLISH_MS      1000
#define LIGHT_CTRL_MS        1000   /* Chu kỳ kiểm tra lịch đèn theo RTC */

/* NTP -> DS3231 RTC sync
 * DS3231 đang được dùng như đồng hồ local UTC+7 cho phase/light schedule.
 * Khi WiFi có Internet, ESP32 lấy giờ NTP, đổi sang UTC+7 rồi ghi vào DS3231 nếu sai lệch > 5s.
 */
#define NTP_SERVER_1              "pool.ntp.org"
#define NTP_SERVER_2              "time.google.com"
#define TZ_INFO_UTC7              "ICT-7"
#define NTP_SYNC_TIMEOUT_MS       30000
#define RTC_NTP_MAX_DRIFT_S       5
#define RTC_NTP_RETRY_MS          (10 * 60 * 1000)
#define RTC_NTP_SYNC_PERIOD_MS    (24 * 60 * 60 * 1000)

/* Local debug logs: hữu ích khi Raspberry Pi Gateway/MQTT chưa bật */
#define SENSOR_LOG_EVERY_N        1    /* 1 = log mỗi gói sensor, tương ứng ~5s */
#define LIGHT_LOG_HEARTBEAT_S     30   /* log trạng thái đèn mỗi 30s nếu không đổi */
#define OFFLINE_LOG_EVERY_N       5    /* khi MQTT mất, log buffer mỗi 5 packet */
#define DRAIN_MAX_PER_CYCLE       5    /* số packet offline đẩy lại mỗi chu kỳ publish */

/* Digital Twin direct safety timeout */
#define DT_PUMP_DEFAULT_DURATION_S   10
#define DT_PUMP_MAX_DURATION_S       15
#define DT_LIGHT_DEFAULT_DURATION_S  300
#define DT_LIGHT_MAX_DURATION_S      1800
#define ACTUATOR_STATE_PUBLISH_MS    5000

/* Phase & Light Schedule */
#define PHASE_1_DARK         1
#define PHASE_2_LIGHT        2
#define DEFAULT_PHASE        PHASE_1_DARK

/* Phase start policy
 * - Phase 0 ngâm hạt nằm ngoài firmware.
 * - planting_start_epoch = thời điểm đã gieo hạt vào đất.
 * - DB/Unity/Web Start là nguồn chuẩn; ESP32 NVS chỉ là cache local để chạy offline.
 * - Reset/mất điện/flash thường: NVS còn, không reset mốc.
 * - Nếu NVS chưa có mốc: KHÔNG tự BOOT_INIT bằng RTC hiện tại; publish valid=false và chờ Gateway SET_EPOCH.
 * - Bắt đầu lứa mới: gửi MQTT TOPIC_CMD_PLANTING_START action=SET_NOW/SET_EPOCH/CLEAR.
 */
#define DARK_PHASE_DAYS     2.0f
#define NVS_NS_PLANTING     "planting"
#define NVS_KEY_START_EPOCH "start_epoch"
#define NVS_KEY_LAST_CMD_ID "last_cmd"
#define PLANTING_CMD_ID_MAX 64
#define DIRECT_CMD_ID_MAX   64

/* Task core pinning */
#define CORE_NET            0
#define CORE_APP            1

#define LIGHT_ON_HOUR_UTC7   8
#define LIGHT_OFF_HOUR_UTC7  16

/* ADS1115 Calibration */
#define SOIL_V_DRY           3.0f
#define SOIL_V_WET           1.1f


/* Sensor filter + soil fusion v2.9.2
 * Sampling target: 1 second.
 * Pipeline: ADS raw -> median samples -> voltage->% -> EMA with recovery -> Schmid-Schossmaier interval fusion.
 * v2.9.2 adds:
 * - Large-change confirm so EMA does not get stuck when sensor is plugged/unplugged.
 * - Soil presence state for OPEN_AIR_OR_DRY / ACTIVE / REMOVED_OR_EXTREME_DRY / SHORT_GND.
 * - Auto pump ON guard when soil is not reliable for plant-control.
 */
#define FILTER_TEMP_ALPHA             0.08f
#define FILTER_HUM_ALPHA              0.08f
#define FILTER_LUX_ALPHA              0.25f
#define FILTER_SOIL_ALPHA             0.10f

#define TEMP_MAX_JUMP_C               3.0f
#define HUM_MAX_JUMP_PERCENT          10.0f
#define LUX_MAX_JUMP_PERCENT          80.0f
#define SOIL_MAX_JUMP_PERCENT         25.0f
#define SOIL_JUMP_CONFIRM_COUNT       3     /* 3 mẫu liên tiếp mới chấp nhận thay đổi lớn */
#define SOIL_RECOVERY_ALPHA           0.40f /* alpha cao khi xác nhận cắm/tháo thật */

#define SOIL_MEDIAN_SAMPLES           5
#define SOIL_SAMPLE_DELAY_MS          12
#define SOIL_FUSION_ERROR_PERCENT     5.0f
#define SOIL_FUSION_FAULT_TOLERANCE   1
#define SOIL_MIN_VALID_COUNT          3
#define SOIL_ZERO_STUCK_LIMIT         30    /* 30s liên tục 0%: cảnh báo stuck/khô cực hạn */
#define SOIL_VOLTAGE_MIN_VALID       -0.05f
#define SOIL_VOLTAGE_MAX_VALID        4.10f
#define SOIL_RAIL_HIGH_V               3.25f  /* Gần 3V3: đất rất khô / chưa cắm đất / open-air */
#define SOIL_RAIL_LOW_V                0.05f  /* Gần 0V: có thể chập GND / lỗi dây */
#define SOIL_REMOVED_CONFIRM_COUNT     5      /* 5s rail-high sau khi từng ACTIVE -> nghi tháo ra */

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
static int  s_current_phase  = DEFAULT_PHASE;   /* phase ESP32 tự tính từ RTC; dùng làm fallback nếu RTC lỗi */
static int64_t s_pump_direct_until_us  = 0;     /* Digital Twin direct pump hold-off */
static int64_t s_light_direct_until_us = 0;     /* Digital Twin direct light hold-off */
static SemaphoreHandle_t s_sensor_mutex;
static SemaphoreHandle_t s_i2c_mutex;
static i2c_dev_t         s_ads_dev = {0};
static i2c_dev_t         s_rtc_dev = {0};
static bool              s_sntp_started = false;
static time_t            s_planting_start_epoch = 0;
static bool              s_planting_start_valid = false;
static char              s_last_planting_cmd_id[PLANTING_CMD_ID_MAX] = {0};

/* v2.9.4: lưu command_id/seq gần nhất của Pump/Light direct để publish actuator_state ACK chính xác.
 * Backend v1.3.0 sẽ ưu tiên ACK theo last_pump_command_id/last_light_command_id.
 */
static char              s_last_pump_cmd_id[DIRECT_CMD_ID_MAX] = {0};
static int               s_last_pump_cmd_seq = 0;
static char              s_last_light_cmd_id[DIRECT_CMD_ID_MAX] = {0};
static int               s_last_light_cmd_seq = 0;

/**
 * @brief Cấu trúc bản ghi chứa toàn bộ dữ liệu Snapshot từ cảm biến
 */
typedef struct {
    float temperature;
    float air_humidity;
    float soil_pct[SOIL_CH_COUNT];          /* Soil sau median + EMA từng kênh */
    float soil_voltage[SOIL_CH_COUNT];      /* Điện áp ADS1115 sau median, để debug calibration */
    float soil_avg_raw;                     /* Trung bình cộng 4 điểm sau lọc */
    float soil_min;
    float soil_max;
    float soil_fused;                       /* Giá trị đại diện bằng Schmid-Schossmaier FTI */
    float soil_fusion_lo;
    float soil_fusion_hi;
    float soil_fusion_confidence;           /* 0.0 .. 1.0 */
    uint8_t  soil_valid_count;
    uint16_t soil_zero_streak;
    bool  soil_reliable;
    bool  soil_stuck_zero;
    bool  soil_saturated_dry;    /* Vraw sát 3V3: đất rất khô/chưa cắm đất, không tự coi là lỗi */
    bool  soil_sensor_fault;     /* Vraw bất thường hoặc stuck không phải rail-high */
    bool  soil_control_reliable; /* TRUE khi dữ liệu soil đủ tin cậy để Gateway AI điều khiển bơm */
    char  soil_presence_state[32]; /* ACTIVE / OPEN_AIR_OR_DRY / REMOVED_OR_EXTREME_DRY / SHORT_GND */
    char  soil_fusion_method[48];
    float lux;
    bool  pump_state;
    bool  light_state;
    int   phase;
    float days_after_planting;
    char  phase_source[24];    /* ESP32_RTC / RTC_ERROR */
    char  pump_mode[24];       /* AI_AUTO / DIRECT_DT */
    char  pump_reason[32];     /* GATEWAY_AI_AUTO / DT_DIRECT / DIRECT_TIMEOUT_OFF */
    char  light_mode[24];      /* AUTO_RTC / DIRECT_DT / LEGACY_CMD */
    char  light_reason[32];    /* PHASE1_LOW_LIGHT / PHASE2_DAYLIGHT_SIM / DT_DIRECT */
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
#define JSON_MAX_LEN         1536

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

static bool i2c_take(uint32_t timeout_ms) {
    if (!s_i2c_mutex) return true;
    return xSemaphoreTake(s_i2c_mutex, pdMS_TO_TICKS(timeout_ms)) == pdTRUE;
}

static void i2c_give(void) {
    if (s_i2c_mutex) xSemaphoreGive(s_i2c_mutex);
}

static bool rtc_get_time_safe(struct tm *out) {
    if (!out) return false;
    bool ok = false;
    if (i2c_take(2500)) {
        ok = (ds3231_get_time(&s_rtc_dev, out) == ESP_OK);
        i2c_give();
    }
    return ok;
}

static bool rtc_set_time_safe(const struct tm *in) {
    if (!in) return false;
    struct tm tmp = *in;
    bool ok = false;
    if (i2c_take(2500)) {
        ok = (ds3231_set_time(&s_rtc_dev, &tmp) == ESP_OK);
        i2c_give();
    }
    return ok;
}

static time_t tm_local_to_epoch(struct tm t) {
    t.tm_isdst = -1;
    return mktime(&t);
}

static bool rtc_get_epoch_from_ds3231(time_t *epoch_out) {
    struct tm t = {0};
    if (!rtc_get_time_safe(&t) || t.tm_year <= 100) return false;
    time_t e = tm_local_to_epoch(t);
    if (e <= 0) return false;
    if (epoch_out) *epoch_out = e;
    return true;
}

static void epoch_to_local_tm(time_t epoch, struct tm *out) {
    localtime_r(&epoch, out);
}

static void epoch_to_iso_utc7(time_t epoch, char *buf, size_t len) {
    if (!buf || len == 0) return;
    if (epoch <= 0) {
        strlcpy(buf, "INVALID", len);
        return;
    }
    struct tm t = {0};
    epoch_to_local_tm(epoch, &t);
    strftime(buf, len, "%Y-%m-%dT%H:%M:%S+07:00", &t);
}

static bool planting_start_load_from_nvs(void) {
    nvs_handle_t h;
    int64_t epoch = 0;
    esp_err_t err = nvs_open(NVS_NS_PLANTING, NVS_READONLY, &h);
    if (err != ESP_OK) return false;

    err = nvs_get_i64(h, NVS_KEY_START_EPOCH, &epoch);

    size_t cmd_len = sizeof(s_last_planting_cmd_id);
    if (nvs_get_str(h, NVS_KEY_LAST_CMD_ID, s_last_planting_cmd_id, &cmd_len) != ESP_OK) {
        s_last_planting_cmd_id[0] = '\0';
    }

    nvs_close(h);

    if (err == ESP_OK && epoch > 0) {
        s_planting_start_epoch = (time_t)epoch;
        s_planting_start_valid = true;
        char iso[32];
        epoch_to_iso_utc7(s_planting_start_epoch, iso, sizeof(iso));
        ESP_LOGI(TAG, "PLANTING: load NVS start_epoch=%lld (%s), last_cmd=%s",
                 (long long)s_planting_start_epoch, iso,
                 s_last_planting_cmd_id[0] ? s_last_planting_cmd_id : "NONE");
        return true;
    }

    return false;
}

static bool planting_start_save_to_nvs(time_t epoch, const char *cmd_id) {
    if (epoch <= 0) return false;

    nvs_handle_t h;
    esp_err_t err = nvs_open(NVS_NS_PLANTING, NVS_READWRITE, &h);
    if (err != ESP_OK) {
        ESP_LOGE(TAG, "PLANTING: nvs_open write lỗi: %s", esp_err_to_name(err));
        return false;
    }

    err = nvs_set_i64(h, NVS_KEY_START_EPOCH, (int64_t)epoch);
    if (err == ESP_OK && cmd_id && cmd_id[0]) {
        err = nvs_set_str(h, NVS_KEY_LAST_CMD_ID, cmd_id);
    }
    if (err == ESP_OK) err = nvs_commit(h);
    nvs_close(h);

    if (err == ESP_OK) {
        s_planting_start_epoch = epoch;
        s_planting_start_valid = true;
        if (cmd_id && cmd_id[0]) strlcpy(s_last_planting_cmd_id, cmd_id, sizeof(s_last_planting_cmd_id));
        char iso[32];
        epoch_to_iso_utc7(epoch, iso, sizeof(iso));
        ESP_LOGW(TAG, "PLANTING: save NVS start_epoch=%lld (%s), cmd_id=%s",
                 (long long)epoch, iso, cmd_id && cmd_id[0] ? cmd_id : "BOOT_INIT");
        return true;
    }

    ESP_LOGE(TAG, "PLANTING: save NVS lỗi: %s", esp_err_to_name(err));
    return false;
}

static bool planting_start_clear_nvs(void) {
    nvs_handle_t h;
    esp_err_t err = nvs_open(NVS_NS_PLANTING, NVS_READWRITE, &h);
    if (err != ESP_OK) return false;

    nvs_erase_key(h, NVS_KEY_START_EPOCH);
    nvs_erase_key(h, NVS_KEY_LAST_CMD_ID);
    err = nvs_commit(h);
    nvs_close(h);

    s_planting_start_epoch = 0;
    s_planting_start_valid = false;
    s_last_planting_cmd_id[0] = '\0';

    return err == ESP_OK;
}

static void rtc_sync_compile_time(void) {
    struct tm t = {0};
    if (!rtc_get_time_safe(&t)) return;

    if (t.tm_year <= 100) {   
        struct tm ct = {0};
        strptime(__DATE__ " " __TIME__, "%b %d %Y %H:%M:%S", &ct);
        if (rtc_set_time_safe(&ct)) {
            ESP_LOGW("RTC", "Đồng bộ RTC từ compile time: %s %s", __DATE__, __TIME__);
        }
    }
}

/**
 * @brief Kiểm tra system time đã hợp lệ sau NTP chưa.
 */
static bool system_time_is_valid(void) {
    time_t now = 0;
    struct tm timeinfo = {0};
    time(&now);
    localtime_r(&now, &timeinfo);
    return (timeinfo.tm_year >= (2024 - 1900));
}

/**
 * @brief Khởi động SNTP một lần. Không gọi lại nhiều lần để tránh lỗi state.
 */
static void ntp_start_once(void) {
    if (s_sntp_started) return;

    setenv("TZ", TZ_INFO_UTC7, 1);
    tzset();

    ESP_LOGI("NTP", "SNTP init: server1=%s server2=%s timezone=%s", NTP_SERVER_1, NTP_SERVER_2, TZ_INFO_UTC7);
    esp_sntp_setoperatingmode(ESP_SNTP_OPMODE_POLL);
    esp_sntp_setservername(0, NTP_SERVER_1);
    esp_sntp_setservername(1, NTP_SERVER_2);
    esp_sntp_init();

    s_sntp_started = true;
}

/**
 * @brief Chờ NTP cập nhật system time.
 */
static bool ntp_wait_time_valid(uint32_t timeout_ms) {
    int waited_ms = 0;
    while (waited_ms < (int)timeout_ms) {
        if (system_time_is_valid()) {
            time_t now = 0;
            struct tm local = {0};
            time(&now);
            localtime_r(&now, &local);
            ESP_LOGI("NTP", "System time valid: %04d-%02d-%02d %02d:%02d:%02d UTC+7",
                     local.tm_year + 1900, local.tm_mon + 1, local.tm_mday,
                     local.tm_hour, local.tm_min, local.tm_sec);
            return true;
        }
        vTaskDelay(pdMS_TO_TICKS(500));
        waited_ms += 500;
    }
    ESP_LOGW("NTP", "Timeout chờ NTP sau %ums", timeout_ms);
    return false;
}

/**
 * @brief Đồng bộ DS3231 từ system time đã được NTP cập nhật.
 * @return true nếu RTC đang đúng hoặc đã set thành công.
 */
static bool rtc_sync_from_ntp_if_needed(void) {
    if (!system_time_is_valid()) {
        ESP_LOGW("RTC", "Không sync DS3231: system time chưa hợp lệ");
        return false;
    }

    time_t now = 0;
    struct tm sys_local = {0};
    time(&now);
    localtime_r(&now, &sys_local);  /* UTC+7 do TZ_INFO_UTC7 */

    struct tm rtc_time = {0};
    bool rtc_read_ok = rtc_get_time_safe(&rtc_time);
    if (!rtc_read_ok || rtc_time.tm_year <= 100) {
        if (rtc_set_time_safe(&sys_local)) {
            ESP_LOGW("RTC", "DS3231 invalid/lỗi đọc -> set từ NTP: %04d-%02d-%02d %02d:%02d:%02d UTC+7",
                     sys_local.tm_year + 1900, sys_local.tm_mon + 1, sys_local.tm_mday,
                     sys_local.tm_hour, sys_local.tm_min, sys_local.tm_sec);
            return true;
        }
        ESP_LOGE("RTC", "Set DS3231 từ NTP thất bại");
        return false;
    }

    time_t rtc_epoch = mktime(&rtc_time);
    time_t sys_epoch = mktime(&sys_local);
    double drift_s = difftime(sys_epoch, rtc_epoch);
    if (drift_s < 0) drift_s = -drift_s;

    if (drift_s > RTC_NTP_MAX_DRIFT_S) {
        if (rtc_set_time_safe(&sys_local)) {
            ESP_LOGW("RTC", "DS3231 lệch %.0fs > %ds -> đã sync từ NTP", drift_s, RTC_NTP_MAX_DRIFT_S);
            return true;
        }
        ESP_LOGE("RTC", "DS3231 lệch %.0fs nhưng set_time thất bại", drift_s);
        return false;
    }

    ESP_LOGI("RTC", "DS3231 OK, lệch %.0fs <= %ds, không cần ghi lại", drift_s, RTC_NTP_MAX_DRIFT_S);
    return true;
}

/**
 * @brief Task định kỳ đồng bộ NTP -> DS3231.
 * - Chạy sau khi WiFi có IP.
 * - Lần đầu sync ngay sau boot nếu có mạng.
 * - Sau đó sync lại mỗi 24h để DS3231 không trôi giờ.
 */
static void ntp_rtc_sync_task(void *pv) {
    (void)pv;
    ESP_LOGI("NTP", "ntp_rtc_sync_task running on core %d", xPortGetCoreID());

    while (1) {
        EventBits_t bits = xEventGroupWaitBits(
            s_wifi_eg,
            WIFI_CONNECTED_BIT,
            pdFALSE,
            pdFALSE,
            pdMS_TO_TICKS(RTC_NTP_RETRY_MS)
        );

        if (!(bits & WIFI_CONNECTED_BIT)) {
            ESP_LOGW("NTP", "Chưa có WiFi, hoãn sync NTP -> DS3231");
            continue;
        }

        ntp_start_once();

        bool ok = false;
        if (ntp_wait_time_valid(NTP_SYNC_TIMEOUT_MS)) {
            ok = rtc_sync_from_ntp_if_needed();
        }

        if (ok) {
            vTaskDelay(pdMS_TO_TICKS(RTC_NTP_SYNC_PERIOD_MS));
        } else {
            ESP_LOGW("NTP", "Sync NTP -> DS3231 thất bại, thử lại sau %d phút", RTC_NTP_RETRY_MS / 60000);
            vTaskDelay(pdMS_TO_TICKS(RTC_NTP_RETRY_MS));
        }
    }
}

static void rtc_get_iso(char *buf, size_t len) {
    struct tm t = {0};
    if (rtc_get_time_safe(&t) && t.tm_year > 100) {
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


/* ================================================================
   SENSOR FILTER + SCHMID-SCHOSSMAIER SOIL FUSION v2.9.2
   ================================================================ */

typedef struct {
    bool initialized;
    float temperature;
    float air_humidity;
    float lux;
    float soil_pct[SOIL_CH_COUNT];
    float soil_voltage[SOIL_CH_COUNT];
    uint8_t soil_jump_count[SOIL_CH_COUNT];
    bool soil_seen_active[SOIL_CH_COUNT];
    uint16_t soil_rail_high_streak[SOIL_CH_COUNT];
    uint16_t soil_rail_low_streak[SOIL_CH_COUNT];
    uint16_t soil_zero_streak;
    char soil_presence_state[32];
} sensor_filter_state_t;

static sensor_filter_state_t s_filter = {0};

static float clampf_local(float x, float lo, float hi) {
    if (x < lo) return lo;
    if (x > hi) return hi;
    return x;
}

static float absf_local(float x) {
    return (x < 0.0f) ? -x : x;
}

static float ema_update(float prev, float cur, float alpha) {
    return alpha * cur + (1.0f - alpha) * prev;
}

static float soil_ema_update_with_recovery(int ch, float previous, float current) {
    float diff = absf_local(current - previous);

    if (diff <= SOIL_MAX_JUMP_PERCENT) {
        s_filter.soil_jump_count[ch] = 0;
        return ema_update(previous, current, FILTER_SOIL_ALPHA);
    }

    /* Nhảy lớn 1-2 mẫu có thể là nhiễu. Nếu kéo dài >= SOIL_JUMP_CONFIRM_COUNT
     * thì xem là thay đổi thật, ví dụ cắm cảm biến vào đất hoặc tháo cảm biến ra.
     */
    if (s_filter.soil_jump_count[ch] < 255) {
        s_filter.soil_jump_count[ch]++;
    }

    if (s_filter.soil_jump_count[ch] >= SOIL_JUMP_CONFIRM_COUNT) {
        s_filter.soil_jump_count[ch] = 0;
        return ema_update(previous, current, SOIL_RECOVERY_ALPHA);
    }

    return previous;
}

static void sort_float_array(float *a, int n) {
    for (int i = 0; i < n - 1; i++) {
        for (int j = i + 1; j < n; j++) {
            if (a[j] < a[i]) {
                float t = a[i];
                a[i] = a[j];
                a[j] = t;
            }
        }
    }
}

static float median_float_array(float *a, int n) {
    sort_float_array(a, n);
    if (n <= 0) return 0.0f;
    if ((n % 2) == 1) return a[n / 2];
    return 0.5f * (a[n / 2 - 1] + a[n / 2]);
}

static float median4_float(float a, float b, float c, float d) {
    float x[4] = {a, b, c, d};
    return median_float_array(x, 4);
}

static bool soil_voltage_valid(float v) {
    return (v >= SOIL_VOLTAGE_MIN_VALID && v <= SOIL_VOLTAGE_MAX_VALID);
}

typedef struct {
    float value;
    float lo;
    float hi;
    float width;
    float confidence;
    uint8_t valid_count;
    bool reliable;
} soil_fusion_result_t;

static soil_fusion_result_t soil_fuse_schmid_schossmaier_f1(const float soil_pct[SOIL_CH_COUNT],
                                                            const float soil_voltage[SOIL_CH_COUNT]) {
    const int n = SOIL_CH_COUNT;
    const int f = SOIL_FUSION_FAULT_TOLERANCE;
    const float err = SOIL_FUSION_ERROR_PERCENT;

    float lo[SOIL_CH_COUNT] = {0};
    float hi[SOIL_CH_COUNT] = {0};
    float s[SOIL_CH_COUNT] = {0};
    uint8_t valid_count = 0;

    for (int i = 0; i < n; i++) {
        s[i] = clampf_local(soil_pct[i], 0.0f, 100.0f);
        if (soil_voltage_valid(soil_voltage[i])) valid_count++;
        lo[i] = clampf_local(s[i] - err, 0.0f, 100.0f);
        hi[i] = clampf_local(s[i] + err, 0.0f, 100.0f);
    }

    sort_float_array(lo, n);
    sort_float_array(hi, n);

    soil_fusion_result_t out = {0};
    out.valid_count = valid_count;

    /* f+1-th largest lower bound and f+1-th smallest upper bound.
     * Với n=4, f=1: lower_idx=2, upper_idx=1.
     */
    int lower_idx = n - f - 1;
    int upper_idx = f;

    out.lo = lo[lower_idx];
    out.hi = hi[upper_idx];

    if (out.lo <= out.hi) {
        float med = median4_float(s[0], s[1], s[2], s[3]);
        out.value = clampf_local(med, out.lo, out.hi);
        out.width = out.hi - out.lo;
        out.confidence = clampf_local(1.0f - (out.width / 100.0f), 0.0f, 1.0f);
        out.reliable = (valid_count >= SOIL_MIN_VALID_COUNT);
    } else {
        /* Khoảng rỗng nghĩa là các cảm biến lệch nhau quá mạnh.
         * Fallback median để không bị trung bình kéo sai, nhưng hạ confidence.
         */
        out.value = median4_float(s[0], s[1], s[2], s[3]);
        out.lo = out.value;
        out.hi = out.value;
        out.width = 100.0f;
        out.confidence = 0.0f;
        out.reliable = false;
    }

    return out;
}

static void apply_sensor_filter_and_fusion(sensor_data_t *s) {
    if (!s) return;

    if (!s_filter.initialized) {
        s_filter.temperature = s->temperature;
        s_filter.air_humidity = s->air_humidity;
        s_filter.lux = s->lux;
        for (int i = 0; i < SOIL_CH_COUNT; i++) {
            s_filter.soil_pct[i] = s->soil_pct[i];
            s_filter.soil_voltage[i] = s->soil_voltage[i];
            s_filter.soil_jump_count[i] = 0;
            s_filter.soil_seen_active[i] = false;
            s_filter.soil_rail_high_streak[i] = 0;
            s_filter.soil_rail_low_streak[i] = 0;
        }
        strlcpy(s_filter.soil_presence_state, "UNKNOWN", sizeof(s_filter.soil_presence_state));
        s_filter.initialized = true;
    }

    if (s->dht11_ok) {
        if (absf_local(s->temperature - s_filter.temperature) <= TEMP_MAX_JUMP_C) {
            s_filter.temperature = ema_update(s_filter.temperature, s->temperature, FILTER_TEMP_ALPHA);
        }
        if (absf_local(s->air_humidity - s_filter.air_humidity) <= HUM_MAX_JUMP_PERCENT) {
            s_filter.air_humidity = ema_update(s_filter.air_humidity, s->air_humidity, FILTER_HUM_ALPHA);
        }
    }

    if (s->bh1750_ok) {
        s_filter.lux = ema_update(s_filter.lux, s->lux, FILTER_LUX_ALPHA);
    }

    uint8_t voltage_valid_count = 0;
    uint8_t active_now_count = 0;
    uint8_t rail_high_count = 0;
    uint8_t rail_low_count = 0;
    uint8_t removed_like_count = 0;
    uint8_t seen_active_count = 0;

    bool all_zero = true;
    float sum = 0.0f;
    float min_v = 100.0f;
    float max_v = 0.0f;

    for (int i = 0; i < SOIL_CH_COUNT; i++) {
        float vraw = s->soil_voltage[i];
        bool v_valid = soil_voltage_valid(vraw);
        bool rail_high = (vraw >= SOIL_RAIL_HIGH_V);
        bool rail_low = (vraw <= SOIL_RAIL_LOW_V);
        bool active_now = v_valid && !rail_high && !rail_low;

        if (v_valid) voltage_valid_count++;
        if (rail_high) rail_high_count++;
        if (rail_low) rail_low_count++;
        if (active_now) active_now_count++;

        if (active_now) {
            s_filter.soil_seen_active[i] = true;
            s_filter.soil_rail_high_streak[i] = 0;
            s_filter.soil_rail_low_streak[i] = 0;
        } else if (rail_high) {
            if (s_filter.soil_rail_high_streak[i] < 60000) s_filter.soil_rail_high_streak[i]++;
            s_filter.soil_rail_low_streak[i] = 0;
        } else if (rail_low) {
            if (s_filter.soil_rail_low_streak[i] < 60000) s_filter.soil_rail_low_streak[i]++;
            s_filter.soil_rail_high_streak[i] = 0;
        }

        if (s_filter.soil_seen_active[i]) seen_active_count++;
        if (s_filter.soil_seen_active[i] &&
            s_filter.soil_rail_high_streak[i] >= SOIL_REMOVED_CONFIRM_COUNT) {
            removed_like_count++;
        }

        /* EMA soil có recovery:
         * - Nhảy nhỏ: EMA bình thường alpha=0.10.
         * - Nhảy lớn 1-2 mẫu: giữ giá trị cũ để chống spike.
         * - Nhảy lớn >=3 mẫu: cập nhật nhanh alpha=0.40 để không kẹt khi cắm/tháo.
         */
        s_filter.soil_pct[i] = soil_ema_update_with_recovery(i,
                                                              s_filter.soil_pct[i],
                                                              s->soil_pct[i]);
        s_filter.soil_voltage[i] = vraw;

        if (s_filter.soil_pct[i] > 0.01f) all_zero = false;
        sum += s_filter.soil_pct[i];
        if (s_filter.soil_pct[i] < min_v) min_v = s_filter.soil_pct[i];
        if (s_filter.soil_pct[i] > max_v) max_v = s_filter.soil_pct[i];
    }

    if (all_zero) {
        if (s_filter.soil_zero_streak < 60000) {
            s_filter.soil_zero_streak++;
        }
    } else {
        s_filter.soil_zero_streak = 0;
    }

    bool all_rail_high = (rail_high_count == SOIL_CH_COUNT);
    bool all_rail_low = (rail_low_count == SOIL_CH_COUNT);
    bool stuck_zero = (s_filter.soil_zero_streak >= SOIL_ZERO_STUCK_LIMIT);
    bool saturated_dry = all_zero && all_rail_high;

    char presence[32] = "UNKNOWN";
    bool sensor_fault = false;

    if (voltage_valid_count < SOIL_MIN_VALID_COUNT) {
        strlcpy(presence, "VOLTAGE_INVALID", sizeof(presence));
        sensor_fault = true;
    } else if (all_rail_low) {
        strlcpy(presence, "SHORT_GND", sizeof(presence));
        sensor_fault = true;
    } else if (active_now_count >= SOIL_MIN_VALID_COUNT) {
        strlcpy(presence, "ACTIVE", sizeof(presence));
    } else if (all_rail_high && seen_active_count >= SOIL_MIN_VALID_COUNT &&
               removed_like_count >= SOIL_MIN_VALID_COUNT) {
        strlcpy(presence, "REMOVED_OR_EXTREME_DRY", sizeof(presence));
    } else if (all_rail_high) {
        strlcpy(presence, "OPEN_AIR_OR_DRY", sizeof(presence));
    } else if (active_now_count > 0) {
        strlcpy(presence, "PARTIAL_ACTIVE", sizeof(presence));
    } else {
        strlcpy(presence, "UNSTABLE", sizeof(presence));
    }

    if (stuck_zero && !saturated_dry) {
        sensor_fault = true;
    }

    soil_fusion_result_t fusion = soil_fuse_schmid_schossmaier_f1(s_filter.soil_pct,
                                                                  s_filter.soil_voltage);

    bool control_reliable = fusion.reliable && !sensor_fault;
    /* Dữ liệu soil chỉ nên dùng cho AI bơm khi ít nhất 3/4 kênh đang ở vùng hoạt động.
     * OPEN_AIR_OR_DRY/REMOVED_OR_EXTREME_DRY không phải lúc nào là lỗi sensor,
     * nhưng không đủ chắc để tự động bật bơm trong lúc test tháo/cắm cảm biến.
     */
    if (strcmp(presence, "ACTIVE") != 0) {
        control_reliable = false;
    }

    s->temperature = clampf_local(s_filter.temperature, 0.0f, 60.0f);
    s->air_humidity = clampf_local(s_filter.air_humidity, 0.0f, 100.0f);
    s->lux = clampf_local(s_filter.lux, 0.0f, 120000.0f);

    for (int i = 0; i < SOIL_CH_COUNT; i++) {
        s->soil_pct[i] = clampf_local(s_filter.soil_pct[i], 0.0f, 100.0f);
        s->soil_voltage[i] = s_filter.soil_voltage[i];
    }

    s->soil_avg_raw = sum / SOIL_CH_COUNT;
    s->soil_min = min_v;
    s->soil_max = max_v;
    s->soil_fused = clampf_local(fusion.value, 0.0f, 100.0f);
    s->soil_fusion_lo = fusion.lo;
    s->soil_fusion_hi = fusion.hi;
    s->soil_fusion_confidence = fusion.confidence;
    s->soil_valid_count = voltage_valid_count;
    s->soil_zero_streak = s_filter.soil_zero_streak;
    s->soil_stuck_zero = stuck_zero;
    s->soil_saturated_dry = saturated_dry;
    s->soil_sensor_fault = sensor_fault;
    s->soil_control_reliable = control_reliable;
    s->soil_reliable = control_reliable;
    strlcpy(s->soil_presence_state, presence, sizeof(s->soil_presence_state));
    strlcpy(s->soil_fusion_method, "schmid_schossmaier_f1_median_ema_recovery", sizeof(s->soil_fusion_method));
}

static bool ads_read_channel_median_voltage(ads111x_mux_t mux, float *voltage_out) {
    if (!voltage_out) return false;

    float samples[SOIL_MEDIAN_SAMPLES] = {0};
    int count = 0;

    for (int k = 0; k < SOIL_MEDIAN_SAMPLES; k++) {
        if (ads111x_set_input_mux(&s_ads_dev, mux) != ESP_OK) return false;
        if (ads111x_start_conversion(&s_ads_dev) != ESP_OK) return false;

        vTaskDelay(pdMS_TO_TICKS(SOIL_SAMPLE_DELAY_MS));

        bool busy = true;
        for (int w = 0; w < 8 && busy; w++) {
            ads111x_is_busy(&s_ads_dev, &busy);
            if (busy) vTaskDelay(pdMS_TO_TICKS(2));
        }

        int16_t raw_v = 0;
        if (ads111x_get_value(&s_ads_dev, &raw_v) == ESP_OK) {
            samples[count++] = (float)((double)raw_v * 4.096 / 32767.0);
        }
    }

    if (count <= 0) return false;
    *voltage_out = median_float_array(samples, count);
    return true;
}

static bool mqtt_topic_match(esp_mqtt_event_handle_t ev, const char *topic) {
    size_t topic_len = strlen(topic);
    return (ev->topic_len == topic_len) &&
           (strncmp(ev->topic, topic, topic_len) == 0);
}

static bool rtc_get_phase_from_planting_time(int *phase_out,
                                             float *days_out,
                                             char *source,
                                             size_t source_len) {
    struct tm now = {0};
    if (!rtc_get_time_safe(&now) || now.tm_year <= 100) {
        if (phase_out) *phase_out = s_current_phase;
        if (days_out)  *days_out = -1.0f;
        if (source) strlcpy(source, "RTC_ERROR", source_len);
        return false;
    }

    if (!s_planting_start_valid || s_planting_start_epoch <= 0) {
        if (phase_out) *phase_out = s_current_phase;
        if (days_out)  *days_out = -1.0f;
        if (source) strlcpy(source, "NVS_NO_START", source_len);
        return false;
    }

    time_t now_epoch = tm_local_to_epoch(now);
    double diff_s = difftime(now_epoch, s_planting_start_epoch);
    float days = (float)(diff_s / 86400.0);
    if (days < 0.0f) days = 0.0f;

    int phase = (days < DARK_PHASE_DAYS) ? PHASE_1_DARK : PHASE_2_LIGHT;

    if (phase_out) *phase_out = phase;
    if (days_out)  *days_out = days;
    if (source) strlcpy(source, "ESP32_RTC_NVS", source_len);
    return true;
}

static bool rtc_get_hour_utc7(int *hour_out) {
    struct tm t = {0};
    if (rtc_get_time_safe(&t) && t.tm_year > 100) {
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
    return snprintf(buf, buf_len,
        "{"
        "\"node_id\":\"%s\","
        "\"fw\":\"%s\","
        "\"timestamp\":\"%s\","
        "\"step\":%d,"
        "\"uptime_s\":%ld,"
        "\"phase\":%d,"
        "\"phase_source\":\"%s\","
        "\"days_after_planting\":%.3f,"
        "\"planting_start_epoch\":%lld,"
        "\"planting_start_valid\":%s,"
        "\"sensor\":{"
            "\"temperature\":%.1f,"
            "\"air_humidity\":%.1f,"
            "\"lux\":%.2f,"
            "\"soil_moisture_avg\":%.1f,"
            "\"soil_moisture_mean\":%.1f,"
            "\"soil_moisture_fused\":%.1f,"
            "\"soil_moisture_min\":%.1f,"
            "\"soil_moisture_max\":%.1f,"
            "\"soil_moisture_raw\":{"
                "\"s1\":%.1f,\"s2\":%.1f,\"s3\":%.1f,\"s4\":%.1f"
            "},"
            "\"soil_voltage_raw\":{"
                "\"v1\":%.3f,\"v2\":%.3f,\"v3\":%.3f,\"v4\":%.3f"
            "},"
            "\"soil_fusion\":{"
                "\"method\":\"%s\","
                "\"lo\":%.1f,"
                "\"hi\":%.1f,"
                "\"confidence\":%.2f,"
                "\"valid_count\":%u,"
                "\"presence_state\":\"%s\","
                "\"control_reliable\":%s,"
                "\"reliable\":%s,"
                "\"stuck_zero\":%s,"
                "\"saturated_dry\":%s,"
                "\"sensor_fault\":%s,"
                "\"zero_streak\":%u"
            "}"
        "},"
        "\"status\":{"
            "\"wifi_rssi\":%d,"
            "\"dht11_ok\":%s,"
            "\"bh1750_ok\":%s,"
            "\"ads1115_ok\":%s,"
            "\"rtc_ok\":%s,"
            "\"pump_on\":%s,"
            "\"pump_mode\":\"%s\","
            "\"pump_reason\":\"%s\","
            "\"light_on\":%s,"
            "\"light_mode\":\"%s\","
            "\"light_reason\":\"%s\""
        "}"
        "}",
        NODE_ID, FW_VERSION, s->iso_time, step,
        s->uptime_s, s->phase,
        s->phase_source[0] ? s->phase_source : "UNKNOWN",
        s->days_after_planting,
        (long long)s_planting_start_epoch,
        s_planting_start_valid ? "true" : "false",
        s->temperature, s->air_humidity, s->lux,
        s->soil_fused, s->soil_avg_raw, s->soil_fused, s->soil_min, s->soil_max,
        s->soil_pct[0], s->soil_pct[1], s->soil_pct[2], s->soil_pct[3],
        s->soil_voltage[0], s->soil_voltage[1], s->soil_voltage[2], s->soil_voltage[3],
        s->soil_fusion_method[0] ? s->soil_fusion_method : "none",
        s->soil_fusion_lo, s->soil_fusion_hi, s->soil_fusion_confidence,
        (unsigned)s->soil_valid_count,
        s->soil_presence_state[0] ? s->soil_presence_state : "UNKNOWN",
        s->soil_control_reliable ? "true" : "false",
        s->soil_reliable ? "true" : "false",
        s->soil_stuck_zero ? "true" : "false",
        s->soil_saturated_dry ? "true" : "false",
        s->soil_sensor_fault ? "true" : "false",
        (unsigned)s->soil_zero_streak,
        s->wifi_rssi,
        s->dht11_ok   ? "true" : "false",
        s->bh1750_ok  ? "true" : "false",
        s->ads1115_ok ? "true" : "false",
        s->rtc_ok     ? "true" : "false",
        s->pump_state ? "true" : "false",
        s->pump_mode[0] ? s->pump_mode : "AI_AUTO",
        s->pump_reason[0] ? s->pump_reason : "UNKNOWN",
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
    if (!s) return 0.0f;
    return s->soil_fused;
}


/* ================================================================
   DIGITAL TWIN DIRECT CONTROL HELPERS
   ================================================================ */

static bool pump_direct_active(void) {
    return (s_pump_direct_until_us > 0 && esp_timer_get_time() < s_pump_direct_until_us);
}

static bool light_direct_active(void) {
    return (s_light_direct_until_us > 0 && esp_timer_get_time() < s_light_direct_until_us);
}

static void mqtt_data_to_cstr(esp_mqtt_event_handle_t ev, char *out, size_t out_len) {
    if (!out || out_len == 0) return;
    size_t n = (ev->data_len < (int)(out_len - 1)) ? ev->data_len : (out_len - 1);
    memcpy(out, ev->data, n);
    out[n] = '\0';
}

static bool json_get_string_value(const char *json, const char *key, char *out, size_t out_len) {
    if (!json || !key || !out || out_len == 0) return false;
    char pattern[32];
    snprintf(pattern, sizeof(pattern), "\"%s\"", key);
    const char *p = strstr(json, pattern);
    if (!p) return false;
    p = strchr(p + strlen(pattern), ':');
    if (!p) return false;
    p++;
    while (*p == ' ' || *p == '\t') p++;
    if (*p != '"') return false;
    p++;
    size_t i = 0;
    while (*p && *p != '"' && i + 1 < out_len) {
        out[i++] = *p++;
    }
    out[i] = '\0';
    return i > 0;
}

static int json_get_int_value(const char *json, const char *key, int default_value) {
    if (!json || !key) return default_value;
    char pattern[32];
    snprintf(pattern, sizeof(pattern), "\"%s\"", key);
    const char *p = strstr(json, pattern);
    if (!p) return default_value;
    p = strchr(p + strlen(pattern), ':');
    if (!p) return default_value;
    p++;
    while (*p == ' ' || *p == '\t') p++;
    return atoi(p);
}

static bool parse_on_off_command(const char *payload, bool default_on) {
    char state[16] = {0};
    if (json_get_string_value(payload, "state", state, sizeof(state)) ||
        json_get_string_value(payload, "cmd", state, sizeof(state)) ||
        json_get_string_value(payload, "action", state, sizeof(state))) {
        return (strcasecmp(state, "ON") == 0 ||
                strcasecmp(state, "PUMP_ON") == 0 ||
                strcasecmp(state, "LIGHT_ON") == 0 ||
                strcmp(state, "1") == 0 ||
                strcasecmp(state, "TRUE") == 0);
    }

    if (strstr(payload, "ON") || strstr(payload, "on") || strstr(payload, "true") || strstr(payload, "1")) {
        return true;
    }
    if (strstr(payload, "OFF") || strstr(payload, "off") || strstr(payload, "false") || strstr(payload, "0")) {
        return false;
    }
    return default_on;
}

static int parse_duration_s(const char *payload, int default_s, int max_s) {
    int d = json_get_int_value(payload, "duration_s", -1);
    if (d < 0) d = json_get_int_value(payload, "duration", -1);
    if (d < 0) d = default_s;
    if (d < 0) d = 0;
    if (d > max_s) d = max_s;
    return d;
}

static int64_t json_get_int64_value(const char *json, const char *key, int64_t default_value) {
    if (!json || !key) return default_value;
    char pattern[40];
    snprintf(pattern, sizeof(pattern), "\"%s\"", key);
    const char *p = strstr(json, pattern);
    if (!p) return default_value;
    p = strchr(p + strlen(pattern), ':');
    if (!p) return default_value;
    p++;
    while (*p == ' ' || *p == '\t' || *p == '"') p++;
    return strtoll(p, NULL, 10);
}

static bool planting_command_is_duplicate(const char *cmd_id) {
    return (cmd_id && cmd_id[0] && s_last_planting_cmd_id[0] &&
            strncmp(cmd_id, s_last_planting_cmd_id, sizeof(s_last_planting_cmd_id)) == 0);
}

static void publish_planting_status(const char *event, const char *cmd_id, const char *status, const char *reason) {
    if (!s_mqtt_connected || !s_mqtt) return;

    char start_iso[32];
    char now_iso[32];
    epoch_to_iso_utc7(s_planting_start_epoch, start_iso, sizeof(start_iso));
    rtc_get_iso(now_iso, sizeof(now_iso));

    char payload[512];
    int len = snprintf(payload, sizeof(payload),
        "{"
        "\"node_id\":\"%s\","
        "\"event\":\"%s\","
        "\"target\":\"planting_start\","
        "\"command_id\":\"%s\","
        "\"status\":\"%s\","
        "\"source\":\"ESP32_NVS\","
        "\"planting_start_epoch\":%lld,"
        "\"planting_start_time\":\"%s\","
        "\"planting_start_valid\":%s,"
        "\"reason\":\"%s\","
        "\"timestamp\":\"%s\""
        "}",
        NODE_ID,
        event ? event : "planting_start_status",
        cmd_id ? cmd_id : "",
        status ? status : "OK",
        (long long)s_planting_start_epoch,
        start_iso,
        s_planting_start_valid ? "true" : "false",
        reason ? reason : "",
        now_iso);

    if (len > 0) esp_mqtt_client_publish(s_mqtt, TOPIC_STATUS_ESP32, payload, len, MQTT_QOS, 0);
}

static bool planting_start_init_from_nvs_or_rtc(void) {
    /* v2.9.3: NVS chỉ là cache local.
     * Không tự tạo start_epoch=now khi NVS rỗng, vì làm che lỗi đồng bộ Unity/DB -> Gateway -> ESP32.
     * Gateway sẽ reconcile DB desired_epoch với ESP32 actual_epoch và gửi SET_EPOCH khi cần.
     */
    if (planting_start_load_from_nvs()) return true;

    s_planting_start_epoch = 0;
    s_planting_start_valid = false;
    s_last_planting_cmd_id[0] = '\0';
    ESP_LOGW(TAG, "PLANTING: NVS chưa có start_epoch -> valid=false, chờ Gateway/Unity SET_EPOCH");
    return false;
}

static bool planting_start_save_verified(time_t epoch, const char *cmd_id) {
    if (epoch <= 0) return false;

    if (!planting_start_save_to_nvs(epoch, cmd_id)) {
        return false;
    }

    /* Đọc lại NVS để xác nhận đã commit đúng epoch. */
    s_planting_start_epoch = 0;
    s_planting_start_valid = false;
    s_last_planting_cmd_id[0] = '\0';

    if (!planting_start_load_from_nvs()) {
        ESP_LOGE(TAG, "PLANTING: verify failed, không đọc lại được NVS sau khi save");
        return false;
    }

    if (!s_planting_start_valid || s_planting_start_epoch != epoch) {
        ESP_LOGE(TAG, "PLANTING: verify mismatch, expected=%lld actual=%lld",
                 (long long)epoch, (long long)s_planting_start_epoch);
        return false;
    }

    return true;
}

static void handle_planting_start_command(esp_mqtt_event_handle_t ev) {
    char payload[320];
    char action[24] = {0};
    char cmd_id[PLANTING_CMD_ID_MAX] = {0};
    char reason[64] = {0};
    mqtt_data_to_cstr(ev, payload, sizeof(payload));

    json_get_string_value(payload, "command_id", cmd_id, sizeof(cmd_id));
    if (!cmd_id[0]) json_get_string_value(payload, "id", cmd_id, sizeof(cmd_id));
    if (!cmd_id[0]) snprintf(cmd_id, sizeof(cmd_id), "mqtt-%lld", (long long)(esp_timer_get_time() / 1000));

    if (planting_command_is_duplicate(cmd_id)) {
        ESP_LOGW(TAG, "PLANTING: bỏ qua command trùng cmd_id=%s", cmd_id);
        publish_planting_status("planting_start_duplicate", cmd_id, "DUPLICATE", "duplicate_command_id");
        return;
    }

    if (!json_get_string_value(payload, "action", action, sizeof(action))) strlcpy(action, "SET_NOW", sizeof(action));
    json_get_string_value(payload, "reason", reason, sizeof(reason));

    if (strcasecmp(action, "SET_NOW") == 0) {
        time_t now_epoch = 0;
        if (!rtc_get_epoch_from_ds3231(&now_epoch)) {
            ESP_LOGE(TAG, "PLANTING: SET_NOW thất bại vì RTC invalid");
            publish_planting_status("planting_start_error", cmd_id, "ERROR", "rtc_invalid");
            return;
        }
        if (planting_start_save_verified(now_epoch, cmd_id)) {
            publish_planting_status("planting_start_updated", cmd_id, "DONE", reason[0] ? reason : "SET_NOW");
        } else {
            publish_planting_status("planting_start_error", cmd_id, "ERROR", "nvs_save_verify_failed");
        }
    } else if (strcasecmp(action, "SET_EPOCH") == 0) {
        int64_t epoch = json_get_int64_value(payload, "planting_start_epoch", 0);
        if (epoch <= 0) epoch = json_get_int64_value(payload, "epoch", 0);
        if (epoch <= 0) {
            publish_planting_status("planting_start_error", cmd_id, "ERROR", "missing_epoch");
            return;
        }
        if (planting_start_save_verified((time_t)epoch, cmd_id)) {
            publish_planting_status("planting_start_updated", cmd_id, "DONE", reason[0] ? reason : "SET_EPOCH");
        } else {
            publish_planting_status("planting_start_error", cmd_id, "ERROR", "nvs_save_verify_failed");
        }
    } else if (strcasecmp(action, "CLEAR") == 0) {
        if (planting_start_clear_nvs()) {
            publish_planting_status("planting_start_cleared", cmd_id, "DONE", reason[0] ? reason : "CLEAR");
        } else {
            publish_planting_status("planting_start_error", cmd_id, "ERROR", "nvs_clear_failed");
        }
    } else if (strcasecmp(action, "GET") == 0) {
        publish_planting_status("planting_start_current", cmd_id, "DONE", reason[0] ? reason : "GET");
    } else {
        ESP_LOGW(TAG, "PLANTING: action không hỗ trợ: %s", action);
        publish_planting_status("planting_start_error", cmd_id, "ERROR", "unsupported_action");
    }
}

static void publish_actuator_state(const char *event_reason) {
    if (!s_mqtt_connected || !s_mqtt) return;

    sensor_data_t snap;
    if (xSemaphoreTake(s_sensor_mutex, pdMS_TO_TICKS(100)) == pdTRUE) {
        snap = g_sensor;
        xSemaphoreGive(s_sensor_mutex);
    } else {
        return;
    }

    char ts[32] = {0};
    rtc_get_iso(ts, sizeof(ts));

    char payload[768];
    int len = snprintf(payload, sizeof(payload),
        "{"
        "\"node_id\":\"%s\","
        "\"timestamp\":\"%s\","
        "\"event\":\"%s\","
        "\"last_pump_command_id\":\"%s\","
        "\"last_pump_seq\":%d,"
        "\"last_light_command_id\":\"%s\","
        "\"last_light_seq\":%d,"
        "\"pump\":{\"state\":\"%s\",\"mode\":\"%s\",\"reason\":\"%s\"},"
        "\"light\":{\"state\":\"%s\",\"mode\":\"%s\",\"reason\":\"%s\"}"
        "}",
        NODE_ID, ts, event_reason ? event_reason : "STATE",
        s_last_pump_cmd_id[0] ? s_last_pump_cmd_id : "",
        s_last_pump_cmd_seq,
        s_last_light_cmd_id[0] ? s_last_light_cmd_id : "",
        s_last_light_cmd_seq,
        snap.pump_state ? "ON" : "OFF",
        snap.pump_mode[0] ? snap.pump_mode : "AI_AUTO",
        snap.pump_reason[0] ? snap.pump_reason : "UNKNOWN",
        snap.light_state ? "ON" : "OFF",
        snap.light_mode[0] ? snap.light_mode : "AUTO_RTC",
        snap.light_reason[0] ? snap.light_reason : "UNKNOWN");

    if (len > 0) {
        esp_mqtt_client_publish(s_mqtt, TOPIC_ACTUATOR_STATE, payload, len, MQTT_QOS, 0);
    }
}

static void set_pump_state(bool on, const char *mode, const char *reason) {
    gpio_set_level(RELAY_PUMP_GPIO, on ? 1 : 0);
    if (xSemaphoreTake(s_sensor_mutex, pdMS_TO_TICKS(100)) == pdTRUE) {
        g_sensor.pump_state = on;
        strlcpy(g_sensor.pump_mode, mode ? mode : "AI_AUTO", sizeof(g_sensor.pump_mode));
        strlcpy(g_sensor.pump_reason, reason ? reason : "UNKNOWN", sizeof(g_sensor.pump_reason));
        xSemaphoreGive(s_sensor_mutex);
    }
}

static void set_light_state(bool on, const char *mode, const char *reason) {
    gpio_set_level(RELAY_LIGHT_GPIO, on ? 1 : 0);
    if (xSemaphoreTake(s_sensor_mutex, pdMS_TO_TICKS(100)) == pdTRUE) {
        g_sensor.light_state = on;
        strlcpy(g_sensor.light_mode, mode ? mode : "AUTO_RTC", sizeof(g_sensor.light_mode));
        strlcpy(g_sensor.light_reason, reason ? reason : "UNKNOWN", sizeof(g_sensor.light_reason));
        xSemaphoreGive(s_sensor_mutex);
    }
}

static void handle_auto_pump_command(esp_mqtt_event_handle_t ev) {
    char payload[256];
    mqtt_data_to_cstr(ev, payload, sizeof(payload));

    if (pump_direct_active()) {
        ESP_LOGW(TAG, "Ignore Gateway auto pump while direct command active: %s", payload);
        return;
    }

    bool on = parse_on_off_command(payload, false);

    if (on) {
        bool soil_ok_for_control = true;
        char presence[32] = "UNKNOWN";
        if (xSemaphoreTake(s_sensor_mutex, pdMS_TO_TICKS(100)) == pdTRUE) {
            soil_ok_for_control = g_sensor.soil_control_reliable;
            strlcpy(presence,
                    g_sensor.soil_presence_state[0] ? g_sensor.soil_presence_state : "UNKNOWN",
                    sizeof(presence));
            xSemaphoreGive(s_sensor_mutex);
        }

        if (!soil_ok_for_control) {
            set_pump_state(false, "AI_AUTO", "SOIL_UNRELIABLE_BLOCK_ON");
            ESP_LOGW(TAG, "Block Gateway AUTO Pump ON: soil not reliable for control, presence=%s payload=%s",
                     presence, payload);
            publish_actuator_state("GATEWAY_CMD_PUMP_BLOCKED");
            return;
        }
    }

    set_pump_state(on, "AI_AUTO", "GATEWAY_AI_AUTO");
    ESP_LOGI(TAG, "Gateway AUTO command: Pump -> %s", on ? "ON" : "OFF");
    publish_actuator_state("GATEWAY_CMD_PUMP");
}

static void handle_dt_pump_command(esp_mqtt_event_handle_t ev) {
    char payload[256];
    char reason[32] = {0};
    char cmd_id[DIRECT_CMD_ID_MAX] = {0};
    mqtt_data_to_cstr(ev, payload, sizeof(payload));

    json_get_string_value(payload, "command_id", cmd_id, sizeof(cmd_id));
    if (!cmd_id[0]) json_get_string_value(payload, "id", cmd_id, sizeof(cmd_id));
    if (cmd_id[0]) strlcpy(s_last_pump_cmd_id, cmd_id, sizeof(s_last_pump_cmd_id));
    s_last_pump_cmd_seq = json_get_int_value(payload, "seq", s_last_pump_cmd_seq);

    bool on = parse_on_off_command(payload, false);
    int duration_s = parse_duration_s(payload, DT_PUMP_DEFAULT_DURATION_S, DT_PUMP_MAX_DURATION_S);
    if (!json_get_string_value(payload, "reason", reason, sizeof(reason))) {
        strlcpy(reason, on ? "DT_DIRECT_ON" : "DT_DIRECT_OFF", sizeof(reason));
    }

    if (on) {
        s_pump_direct_until_us = esp_timer_get_time() + ((int64_t)duration_s * 1000000LL);
        set_pump_state(true, "DIRECT_DT", reason);
    } else {
        s_pump_direct_until_us = esp_timer_get_time() + 2000000LL;  /* giữ quyền 2s để tránh Gateway gửi ngược ngay */
        set_pump_state(false, "DIRECT_DT", reason);
    }

    ESP_LOGW(TAG, "DT command: Pump -> %s duration=%ds reason=%s cmd_id=%s seq=%d",
             on ? "ON" : "OFF", duration_s, reason,
             s_last_pump_cmd_id[0] ? s_last_pump_cmd_id : "NONE", s_last_pump_cmd_seq);
    publish_actuator_state("DT_CMD_PUMP");
}

static void handle_dt_light_command(esp_mqtt_event_handle_t ev, const char *mode_name) {
    char payload[256];
    char reason[32] = {0};
    char cmd_id[DIRECT_CMD_ID_MAX] = {0};
    mqtt_data_to_cstr(ev, payload, sizeof(payload));

    json_get_string_value(payload, "command_id", cmd_id, sizeof(cmd_id));
    if (!cmd_id[0]) json_get_string_value(payload, "id", cmd_id, sizeof(cmd_id));
    if (cmd_id[0]) strlcpy(s_last_light_cmd_id, cmd_id, sizeof(s_last_light_cmd_id));
    s_last_light_cmd_seq = json_get_int_value(payload, "seq", s_last_light_cmd_seq);

    bool on = parse_on_off_command(payload, false);
    int duration_s = parse_duration_s(payload, DT_LIGHT_DEFAULT_DURATION_S, DT_LIGHT_MAX_DURATION_S);
    if (!json_get_string_value(payload, "reason", reason, sizeof(reason))) {
        strlcpy(reason, on ? "DT_LIGHT_ON" : "DT_LIGHT_OFF", sizeof(reason));
    }

    if (on) {
        s_light_direct_until_us = esp_timer_get_time() + ((int64_t)duration_s * 1000000LL);
        set_light_state(true, mode_name ? mode_name : "DIRECT_DT", reason);
    } else {
        s_light_direct_until_us = esp_timer_get_time() + 2000000LL;
        set_light_state(false, mode_name ? mode_name : "DIRECT_DT", reason);
    }

    ESP_LOGW(TAG, "%s command: Light -> %s duration=%ds reason=%s cmd_id=%s seq=%d",
             mode_name ? mode_name : "DT", on ? "ON" : "OFF", duration_s, reason,
             s_last_light_cmd_id[0] ? s_last_light_cmd_id : "NONE", s_last_light_cmd_seq);
    publish_actuator_state("DT_CMD_LIGHT");
}

static void log_sensor_snapshot(const sensor_data_t *s, int step, int ring_used) {
    ESP_LOGI(TAG,
             "SENSOR step=%d phase=%d(%s) source=%s days=%.2f rtc=%s time=%s | "
             "T=%.1fC RH=%.1f%% Lux=%.2f SoilFused=%.1f%% Avg=%.1f%% Min=%.1f%% Max=%.1f%% "
             "S[%.1f %.1f %.1f %.1f] V[%.3f %.3f %.3f %.3f] "
             "fusion=[%.1f..%.1f] conf=%.2f presence=%s reliable=%s control=%s valid=%u stuck0=%s/%u dry=%s fault=%s | "
             "Pump=%s Light=%s/%s reason=%s | RSSI=%d MQTT=%s Buffer=%d/%d",
             step, s->phase, phase_to_text(s->phase),
             s->phase_source[0] ? s->phase_source : "UNKNOWN",
             s->days_after_planting,
             s->rtc_ok ? "OK" : "ERR", s->iso_time,
             s->temperature, s->air_humidity, s->lux,
             s->soil_fused, s->soil_avg_raw, s->soil_min, s->soil_max,
             s->soil_pct[0], s->soil_pct[1], s->soil_pct[2], s->soil_pct[3],
             s->soil_voltage[0], s->soil_voltage[1], s->soil_voltage[2], s->soil_voltage[3],
             s->soil_fusion_lo, s->soil_fusion_hi, s->soil_fusion_confidence,
             s->soil_presence_state[0] ? s->soil_presence_state : "UNKNOWN",
             s->soil_reliable ? "YES" : "NO",
             s->soil_control_reliable ? "YES" : "NO",
             (unsigned)s->soil_valid_count,
             s->soil_stuck_zero ? "YES" : "NO",
             (unsigned)s->soil_zero_streak,
             s->soil_saturated_dry ? "YES" : "NO",
             s->soil_sensor_fault ? "YES" : "NO",
             s->pump_state ? "ON" : "OFF",
             s->light_state ? "ON" : "OFF",
             s->light_mode[0] ? s->light_mode : "AUTO_RTC",
             s->light_reason[0] ? s->light_reason : "UNKNOWN",
             s->wifi_rssi, s_mqtt_connected ? "ON" : "OFF",
             ring_used, RING_BUF_SIZE);
}

/* ================================================================
   WIFI EVENT HANDLER DÀNH CHO MULTI-WIFI — SAFE SCAN, KHÔNG CRASH
   ================================================================ */

static const char *wifi_reason_to_text(uint8_t reason) {
    /* Dùng numeric code để tương thích nhiều phiên bản ESP-IDF.
       Các mã hay gặp:
       15  = 4-way handshake timeout
       200 = beacon timeout
       201 = no AP found
       202 = auth fail / sai mật khẩu hoặc security không tương thích
       203 = association fail
       204 = handshake timeout
    */
    switch (reason) {
        case 2:   return "AUTH_EXPIRE";
        case 3:   return "AUTH_LEAVE";
        case 4:   return "ASSOC_EXPIRE";
        case 5:   return "ASSOC_TOOMANY";
        case 6:   return "NOT_AUTHED";
        case 7:   return "NOT_ASSOCED";
        case 8:   return "ASSOC_LEAVE";
        case 9:   return "ASSOC_NOT_AUTHED";
        case 10:  return "DISASSOC_PWRCAP_BAD";
        case 11:  return "DISASSOC_SUPCHAN_BAD";
        case 13:  return "IE_INVALID";
        case 14:  return "MIC_FAILURE";
        case 15:  return "4WAY_HANDSHAKE_TIMEOUT";
        case 16:  return "GROUP_KEY_UPDATE_TIMEOUT";
        case 17:  return "IE_IN_4WAY_DIFFERS";
        case 18:  return "GROUP_CIPHER_INVALID";
        case 19:  return "PAIRWISE_CIPHER_INVALID";
        case 20:  return "AKMP_INVALID";
        case 21:  return "UNSUPP_RSN_IE_VERSION";
        case 22:  return "INVALID_RSN_IE_CAP";
        case 23:  return "802_1X_AUTH_FAILED";
        case 24:  return "CIPHER_SUITE_REJECTED";
        case 200: return "BEACON_TIMEOUT";
        case 201: return "NO_AP_FOUND";
        case 202: return "AUTH_FAIL";
        case 203: return "ASSOC_FAIL";
        case 204: return "HANDSHAKE_TIMEOUT";
        default:  return "UNKNOWN";
    }
}

static const char *wifi_auth_to_text(wifi_auth_mode_t auth) {
    switch (auth) {
        case WIFI_AUTH_OPEN:            return "OPEN";
        case WIFI_AUTH_WEP:             return "WEP";
        case WIFI_AUTH_WPA_PSK:         return "WPA";
        case WIFI_AUTH_WPA2_PSK:        return "WPA2";
        case WIFI_AUTH_WPA_WPA2_PSK:    return "WPA/WPA2";
        case WIFI_AUTH_WPA2_ENTERPRISE: return "WPA2_ENT";
        case WIFI_AUTH_WPA3_PSK:        return "WPA3";
        case WIFI_AUTH_WPA2_WPA3_PSK:   return "WPA2/WPA3";
        default:                        return "UNKNOWN";
    }
}

static bool wifi_apply_config(int index) {
    if (index < 0 || index >= (int)WIFI_NETWORK_COUNT) {
        ESP_LOGE(TAG, "WiFi index không hợp lệ: %d", index);
        return false;
    }

    wifi_config_t wifi_config = {0};
    strlcpy((char *)wifi_config.sta.ssid,
            WIFI_NETWORKS[index].ssid,
            sizeof(wifi_config.sta.ssid));
    strlcpy((char *)wifi_config.sta.password,
            WIFI_NETWORKS[index].password,
            sizeof(wifi_config.sta.password));

    /* Để tương thích router/hotspot WPA/WPA2/WPA3 mixed tốt hơn, không ép quá cứng WPA2. */
    wifi_config.sta.threshold.authmode = WIFI_AUTH_WPA_PSK;
    wifi_config.sta.scan_method = WIFI_ALL_CHANNEL_SCAN;
    wifi_config.sta.sort_method = WIFI_CONNECT_AP_BY_SIGNAL;

    esp_err_t err = esp_wifi_set_config(WIFI_IF_STA, &wifi_config);
    if (err != ESP_OK) {
        ESP_LOGE(TAG, "esp_wifi_set_config(%s) lỗi: %s",
                 WIFI_NETWORKS[index].ssid, esp_err_to_name(err));
        return false;
    }

    s_current_wifi_index = index;
    ESP_LOGI(TAG, "WiFi config: SSID=%s", WIFI_NETWORKS[index].ssid);
    return true;
}

/**
 * @brief Scan WiFi an toàn. Không dùng ESP_ERROR_CHECK vì scan có thể timeout tạm thời.
 * @return index mạng quen thuộc có RSSI tốt nhất; nếu không scan được hoặc không thấy thì trả về index hiện tại.
 */
static int wifi_scan_select_best_known(void) {
    wifi_ap_record_t ap_records[WIFI_SCAN_MAX_AP];
    uint16_t ap_count = WIFI_SCAN_MAX_AP;
    memset(ap_records, 0, sizeof(ap_records));

    wifi_scan_config_t scan_config = {
        .ssid = NULL,
        .bssid = NULL,
        .channel = 0,              /* 0 = quét tất cả channel */
        .show_hidden = true,
        .scan_type = WIFI_SCAN_TYPE_ACTIVE,
        .scan_time.active.min = WIFI_SCAN_MIN_MS,
        .scan_time.active.max = WIFI_SCAN_MAX_MS,
    };

    ESP_LOGI(TAG, "Scan WiFi an toàn: all channel, max_ap=%d, active=%d-%dms",
             WIFI_SCAN_MAX_AP, WIFI_SCAN_MIN_MS, WIFI_SCAN_MAX_MS);

    esp_err_t err = esp_wifi_scan_start(&scan_config, true);
    if (err != ESP_OK) {
        ESP_LOGW(TAG, "Scan WiFi lỗi: %s — bỏ qua scan, vẫn connect trực tiếp SSID đã cấu hình",
                 esp_err_to_name(err));
        return s_current_wifi_index;
    }

    err = esp_wifi_scan_get_ap_records(&ap_count, ap_records);
    if (err != ESP_OK) {
        ESP_LOGW(TAG, "Lấy AP records lỗi: %s — bỏ qua scan", esp_err_to_name(err));
        return s_current_wifi_index;
    }

    ESP_LOGI(TAG, "Scan thấy %d mạng WiFi:", ap_count);

    int best_index = -1;
    int best_rssi = -999;

    for (int i = 0; i < ap_count; i++) {
        const char *ssid = (const char *)ap_records[i].ssid;
        const char *print_ssid = (ssid && ssid[0]) ? ssid : "<hidden>";
        ESP_LOGI(TAG, "  [%02d] SSID=%s | RSSI=%d | CH=%d | AUTH=%s",
                 i + 1, print_ssid, ap_records[i].rssi,
                 ap_records[i].primary, wifi_auth_to_text(ap_records[i].authmode));

        for (int k = 0; k < (int)WIFI_NETWORK_COUNT; k++) {
            if (ssid && strcmp(ssid, WIFI_NETWORKS[k].ssid) == 0) {
                if (ap_records[i].rssi > best_rssi) {
                    best_rssi = ap_records[i].rssi;
                    best_index = k;
                }
            }
        }
    }

    if (best_index >= 0) {
        ESP_LOGI(TAG, "Chọn WiFi quen thuộc mạnh nhất: %s (RSSI=%d)",
                 WIFI_NETWORKS[best_index].ssid, best_rssi);
        return best_index;
    }

    ESP_LOGW(TAG, "Không thấy SSID quen thuộc trong scan — vẫn thử connect trực tiếp SSID hiện tại: %s",
             WIFI_NETWORKS[s_current_wifi_index].ssid);
    return s_current_wifi_index;
}

static void wifi_connect_current(void) {
    esp_err_t err = esp_wifi_connect();
    if (err != ESP_OK) {
        ESP_LOGW(TAG, "esp_wifi_connect(%s) lỗi mềm: %s",
                 WIFI_NETWORKS[s_current_wifi_index].ssid, esp_err_to_name(err));
    }
}

/**
 * @brief Chuyển đổi sang điểm truy cập WiFi tiếp theo trong danh sách, không crash nếu set_config lỗi.
 */
static void wifi_switch_to_next_network(void) {
    int next = (s_current_wifi_index + 1) % WIFI_NETWORK_COUNT;
    s_wifi_retry_count = 0;

    ESP_LOGI(TAG, "==> Đổi sang WiFi tiếp theo: %s", WIFI_NETWORKS[next].ssid);
    if (wifi_apply_config(next)) {
        wifi_connect_current();
    }

    /* Nếu đã duyệt hết 1 vòng mảng WiFi mà vẫn chưa kết nối, báo offline nhưng vẫn tiếp tục retry. */
    if (next == 0) {
        xEventGroupSetBits(s_wifi_eg, WIFI_FAIL_BIT);
    }
}

static void wifi_event_handler(void *arg, esp_event_base_t base,
                                int32_t id, void *data) {
    if (base == WIFI_EVENT && id == WIFI_EVENT_STA_START) {
        ESP_LOGI(TAG, "WiFi STA started");
    } else if (base == WIFI_EVENT && id == WIFI_EVENT_STA_DISCONNECTED) {
        s_mqtt_connected = false;
        xEventGroupClearBits(s_wifi_eg, WIFI_CONNECTED_BIT);

        wifi_event_sta_disconnected_t *event = (wifi_event_sta_disconnected_t *)data;
        uint8_t reason = event ? event->reason : 0;

        ESP_LOGW(TAG, "WiFi disconnected | SSID=%s | reason=%u(%s) | retry=%d/%d",
                 WIFI_NETWORKS[s_current_wifi_index].ssid,
                 reason, wifi_reason_to_text(reason),
                 s_wifi_retry_count, WIFI_MAX_RETRY);

        if (s_wifi_retry_count < WIFI_MAX_RETRY) {
            s_wifi_retry_count++;
            ESP_LOGW(TAG, "Thử kết nối lại %d/%d với SSID: %s",
                     s_wifi_retry_count, WIFI_MAX_RETRY,
                     WIFI_NETWORKS[s_current_wifi_index].ssid);
            wifi_connect_current();
        } else {
            ESP_LOGE(TAG, "Kết nối [%s] thất bại sau %d lần — đổi mạng",
                     WIFI_NETWORKS[s_current_wifi_index].ssid, WIFI_MAX_RETRY);
            wifi_switch_to_next_network();
        }
    } else if (base == IP_EVENT && id == IP_EVENT_STA_GOT_IP) {
        ip_event_got_ip_t *event = (ip_event_got_ip_t *) data;
        ESP_LOGI(TAG, "WiFi connected | IP=" IPSTR " | GW=" IPSTR " | SSID=%s",
                 IP2STR(&event->ip_info.ip), IP2STR(&event->ip_info.gw),
                 WIFI_NETWORKS[s_current_wifi_index].ssid);
        s_wifi_retry_count = 0;
        xEventGroupClearBits(s_wifi_eg, WIFI_FAIL_BIT);
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

    ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_STA));

    /* Việt Nam dùng channel 1-13. Nếu để mặc định US, mạng ở channel 12/13 có thể không thấy. */
    wifi_country_t country = {
        .cc = "VN",
        .schan = 1,
        .nchan = 13,
        .policy = WIFI_COUNTRY_POLICY_MANUAL,
    };
    esp_err_t country_err = esp_wifi_set_country(&country);
    if (country_err != ESP_OK) {
        ESP_LOGW(TAG, "set_country(VN) lỗi mềm: %s", esp_err_to_name(country_err));
    }

    /* Đặt cấu hình mặc định trước khi start. */
    wifi_apply_config(s_current_wifi_index);

    ESP_ERROR_CHECK(esp_wifi_start());
    vTaskDelay(pdMS_TO_TICKS(500));

    /* Scan chỉ để chọn SSID tốt hơn và debug. Nếu scan timeout thì không crash. */
    int selected = wifi_scan_select_best_known();
    wifi_apply_config(selected);
    wifi_connect_current();

    EventBits_t bits = xEventGroupWaitBits(s_wifi_eg,
        WIFI_CONNECTED_BIT | WIFI_FAIL_BIT, pdFALSE, pdFALSE,
        pdMS_TO_TICKS(WIFI_BOOT_WAIT_MS));

    if (bits & WIFI_CONNECTED_BIT) {
        ESP_LOGI(TAG, "Boot WiFi hoàn tất");
    } else {
        ESP_LOGW(TAG, "WiFi boot chưa kết nối sau %dms — chuyển sang Offline, task WiFi vẫn tiếp tục retry",
                 WIFI_BOOT_WAIT_MS);
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
            esp_mqtt_client_subscribe(s_mqtt, TOPIC_CMD_PUMP_AUTO, MQTT_QOS);
            esp_mqtt_client_subscribe(s_mqtt, TOPIC_CMD_PUMP_DIRECT, MQTT_QOS);
            esp_mqtt_client_subscribe(s_mqtt, TOPIC_CMD_LIGHT_DIRECT, MQTT_QOS);
            esp_mqtt_client_subscribe(s_mqtt, TOPIC_CMD_PLANTING_START, MQTT_QOS);
            ESP_LOGI(TAG,
                     "MQTT connected %s:%d — sub: auto/pump, direct/pump, direct/light, config/planting_start | buffer=%d/%d",
                     MQTT_BROKER_URI, MQTT_PORT, ring_count(), RING_BUF_SIZE);
            publish_actuator_state("MQTT_CONNECTED");
            publish_planting_status("planting_start_current", "", "DONE", "MQTT_CONNECTED");
            break;
        case MQTT_EVENT_DISCONNECTED:
            s_mqtt_connected = false;
            ESP_LOGW(TAG, "MQTT mất kết nối — buffering ON | broker=%s:%d | buffer=%d/%d",
                     MQTT_BROKER_URI, MQTT_PORT, ring_count(), RING_BUF_SIZE);
            break;
        case MQTT_EVENT_ERROR:
            s_mqtt_connected = false;
            ESP_LOGE(TAG, "MQTT_EVENT_ERROR — chưa kết nối được broker/Raspberry Pi Gateway? broker=%s:%d | buffer=%d/%d",
                     MQTT_BROKER_URI, MQTT_PORT, ring_count(), RING_BUF_SIZE);
            break;
        case MQTT_EVENT_DATA:
            if (mqtt_topic_match(ev, TOPIC_CMD_PUMP_AUTO)) {
                handle_auto_pump_command(ev);
            } else if (mqtt_topic_match(ev, TOPIC_CMD_PUMP_DIRECT)) {
                handle_dt_pump_command(ev);
            } else if (mqtt_topic_match(ev, TOPIC_CMD_LIGHT_DIRECT)) {
                handle_dt_light_command(ev, "DIRECT_DT");
            } else if (mqtt_topic_match(ev, TOPIC_CMD_PLANTING_START)) {
                handle_planting_start_command(ev);
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
        .credentials.client_id = MQTT_CLIENT_ID,
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
        g_sensor.days_after_planting = 0.0f;
        strlcpy(g_sensor.phase_source, "BOOT_DEFAULT", sizeof(g_sensor.phase_source));
        strlcpy(g_sensor.pump_mode, "AI_AUTO", sizeof(g_sensor.pump_mode));
        strlcpy(g_sensor.pump_reason, "BOOT_OFF", sizeof(g_sensor.pump_reason));
        strlcpy(g_sensor.light_mode, "AUTO_RTC", sizeof(g_sensor.light_mode));
        strlcpy(g_sensor.light_reason, "PHASE1_LOW_LIGHT", sizeof(g_sensor.light_reason));
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
    ESP_LOGI(TAG, "sensor_task running on core %d", xPortGetCoreID());
    dht11_t dht = { .dht11_pin = DHT11_GPIO };
    i2c_dev_t bh_dev = {0};
    bool bh_ready = false, ads_ready = false;

    if (i2c_take(1500)) {
        if (bh1750_init_desc(&bh_dev, BH1750_I2C_ADDR, I2C_MASTER_PORT, I2C_MASTER_SDA, I2C_MASTER_SCL) == ESP_OK
            && bh1750_power_on(&bh_dev) == ESP_OK
            && bh1750_setup(&bh_dev, BH1750_MODE_CONTINUOUS, BH1750_RES_HIGH2) == ESP_OK) {
            bh_ready = true;
        } else {
            ESP_LOGW(TAG, "BH1750 init/setup chưa sẵn sàng");
        }
        i2c_give();
    } else {
        ESP_LOGW(TAG, "BH1750 init skip: I2C busy");
    }

    if (i2c_take(1500)) {
        if (ads111x_init_desc(&s_ads_dev, ADS1115_I2C_ADDR, I2C_MASTER_PORT, I2C_MASTER_SDA, I2C_MASTER_SCL) == ESP_OK
            && ads111x_set_mode(&s_ads_dev, ADS111X_MODE_SINGLE_SHOT) == ESP_OK
            && ads111x_set_data_rate(&s_ads_dev, ADS111X_DATA_RATE_128) == ESP_OK
            && ads111x_set_gain(&s_ads_dev, ADS111X_GAIN_4V096) == ESP_OK) {
            ads_ready = true;
        } else {
            ESP_LOGW(TAG, "ADS1115 init/setup chưa sẵn sàng");
        }
        i2c_give();
    } else {
        ESP_LOGW(TAG, "ADS1115 init skip: I2C busy");
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
            if (i2c_take(600)) {
                if (bh1750_read(&bh_dev, &raw) == ESP_OK) {
                    snap.lux = (float)raw / 2.0f;
                    snap.bh1750_ok = true;
                }
                i2c_give();
            }
        }

        if (ads_ready) {
            bool all_ok = true;
            if (i2c_take(3000)) {
                for (int i = 0; i < SOIL_CH_COUNT; i++) {
                    float voltage = 0.0f;
                    if (ads_read_channel_median_voltage(SOIL_MUX[i], &voltage)) {
                        snap.soil_voltage[i] = voltage;
                        snap.soil_pct[i] = ads_voltage_to_pct(voltage);
                    } else {
                        all_ok = false;
                    }
                }
                i2c_give();
            } else {
                all_ok = false;
                ESP_LOGW(TAG, "ADS1115 skip: I2C busy");
            }
            snap.ads1115_ok = all_ok;
        }

        apply_sensor_filter_and_fusion(&snap);

        wifi_ap_record_t ap;
        if (esp_wifi_sta_get_ap_info(&ap) == ESP_OK) snap.wifi_rssi = ap.rssi;

        if (xSemaphoreTake(s_sensor_mutex, pdMS_TO_TICKS(100)) == pdTRUE) {
            snap.pump_state = g_sensor.pump_state;
            strlcpy(snap.pump_mode,
                    g_sensor.pump_mode[0] ? g_sensor.pump_mode : "AI_AUTO",
                    sizeof(snap.pump_mode));
            strlcpy(snap.pump_reason,
                    g_sensor.pump_reason[0] ? g_sensor.pump_reason : "UNKNOWN",
                    sizeof(snap.pump_reason));
            snap.light_state = g_sensor.light_state;
            snap.phase       = g_sensor.phase ? g_sensor.phase : DEFAULT_PHASE;
            snap.days_after_planting = g_sensor.days_after_planting;
            strlcpy(snap.phase_source,
                    g_sensor.phase_source[0] ? g_sensor.phase_source : "UNKNOWN",
                    sizeof(snap.phase_source));
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
 * @brief Task: ESP32 là nguồn phase chính.
 *
 * Luồng hiện tại:
 * - ESP32 đọc DS3231 RTC và tính days_after_planting từ planting_start_epoch lưu trong NVS. 
 * - Nếu days_after_planting < DARK_PHASE_DAYS => Phase 1.
 * - Nếu days_after_planting >= DARK_PHASE_DAYS => Phase 2.
 * - ESP32 tự bật/tắt đèn theo phase và lịch RTC.
 * - Gateway chỉ nhận phase trong JSON để chạy AI/ghi log, không gửi cmd/phase liên tục.
 */
static void light_schedule_task(void *pv) {
    ESP_LOGI(TAG, "light_schedule_task running on core %d", xPortGetCoreID());
    bool last_light_on = false;
    int  last_phase = -1;
    char last_reason[24] = {0};
    int heartbeat_tick = 0;

    while (1) {
        int phase = s_current_phase;
        float days_after_planting = -1.0f;
        char phase_source[24] = {0};
        bool rtc_phase_ok = rtc_get_phase_from_planting_time(&phase,
                                                             &days_after_planting,
                                                             phase_source,
                                                             sizeof(phase_source));
        s_current_phase = phase;

        int hour_utc7 = -1;
        bool rtc_hour_ok = rtc_get_hour_utc7(&hour_utc7);
        bool rtc_ok = rtc_phase_ok && rtc_hour_ok;

        /* Nếu Digital Twin đang điều khiển đèn trực tiếp thì không cho AUTO_RTC ghi đè. */
        if (light_direct_active()) {
            if (xSemaphoreTake(s_sensor_mutex, pdMS_TO_TICKS(100)) == pdTRUE) {
                g_sensor.phase = phase;
                g_sensor.days_after_planting = days_after_planting;
                strlcpy(g_sensor.phase_source, phase_source, sizeof(g_sensor.phase_source));
                g_sensor.rtc_ok = rtc_ok;
                xSemaphoreGive(s_sensor_mutex);
            }
            if (++heartbeat_tick >= (LIGHT_LOG_HEARTBEAT_S * 1000 / LIGHT_CTRL_MS)) {
                ESP_LOGW(TAG, "LIGHT AUTO_RTC paused: DIRECT_DT active, phase=%d days=%.2f",
                         phase, days_after_planting);
                heartbeat_tick = 0;
            }
            vTaskDelay(pdMS_TO_TICKS(LIGHT_CTRL_MS));
            continue;
        }

        if (s_light_direct_until_us > 0 && !light_direct_active()) {
            s_light_direct_until_us = 0;
            ESP_LOGI(TAG, "LIGHT DIRECT expired -> return AUTO_RTC");
        }

        bool light_on = false;
        const char *reason = "RTC_ERROR";

        if (rtc_hour_ok) {
            light_on = light_should_be_on_by_schedule(phase, hour_utc7);
            reason = (phase == PHASE_1_DARK) ? "PHASE1_LOW_LIGHT" :
                     (light_on ? "PHASE2_DAYLIGHT_SIM" : "PHASE2_NIGHT_SIM");
        }

        gpio_set_level(RELAY_LIGHT_GPIO, light_on ? 1 : 0);

        if (xSemaphoreTake(s_sensor_mutex, pdMS_TO_TICKS(100)) == pdTRUE) {
            g_sensor.phase = phase;
            g_sensor.days_after_planting = days_after_planting;
            strlcpy(g_sensor.phase_source, phase_source, sizeof(g_sensor.phase_source));
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
            ESP_LOGI(TAG, "LIGHT phase=%d(%s) source=%s days=%.2f rtc=%s hour=%d schedule=%02d-%02d => light=%s reason=%s",
                     phase, phase_to_text(phase), phase_source, days_after_planting,
                     rtc_ok ? "OK" : "ERR", hour_utc7,
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
 * @brief Task: Bảo vệ actuator direct mode và publish trạng thái relay thật.
 *
 * - Nếu Digital Twin bật bơm với duration_s, ESP32 tự tắt khi hết thời gian.
 * - Publish định kỳ actuator/state để Gateway/Influx xác nhận trạng thái vật lý.
 */
static void actuator_state_task(void *pv) {
    ESP_LOGI(TAG, "actuator_state_task running on core %d", xPortGetCoreID());
    int64_t last_pub_us = 0;

    while (1) {
        int64_t now = esp_timer_get_time();

        if (s_pump_direct_until_us > 0 && now >= s_pump_direct_until_us) {
            bool was_on = false;
            if (xSemaphoreTake(s_sensor_mutex, pdMS_TO_TICKS(100)) == pdTRUE) {
                was_on = g_sensor.pump_state;
                xSemaphoreGive(s_sensor_mutex);
            }

            if (was_on) {
                set_pump_state(false, "AI_AUTO", "DIRECT_TIMEOUT_OFF");
                ESP_LOGW(TAG, "Pump DIRECT timeout -> OFF, return AI_AUTO");
                publish_actuator_state("PUMP_DIRECT_TIMEOUT");
            }
            s_pump_direct_until_us = 0;
        }

        if (s_light_direct_until_us > 0 && now >= s_light_direct_until_us) {
            s_light_direct_until_us = 0;
            ESP_LOGI(TAG, "Light DIRECT timeout -> AUTO_RTC will resume by schedule task");
            publish_actuator_state("LIGHT_DIRECT_TIMEOUT");
        }

        if (s_mqtt_connected && (now - last_pub_us) >= ((int64_t)ACTUATOR_STATE_PUBLISH_MS * 1000LL)) {
            publish_actuator_state("PERIODIC_STATE");
            last_pub_us = now;
        }

        vTaskDelay(pdMS_TO_TICKS(500));
    }
}

/**
 * @brief Task: Xuất dữ liệu lên MQTT và giải phóng bộ đệm (Drain Buffer).
 */
static void publish_task(void *pv) {
    ESP_LOGI(TAG, "publish_task running on core %d", xPortGetCoreID());
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
                ESP_LOGW(TAG, "OFFLINE step=%d — Raspberry Pi Gateway/MQTT chưa sẵn sàng, lưu packet vào ring buffer %d/%d",
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
    ESP_LOGI(TAG, "MQTT broker=%s:%d | client_id=%s", MQTT_BROKER_URI, MQTT_PORT, MQTT_CLIENT_ID);
    ESP_LOGI(TAG, "Topics v2 | sensor=%s | esp32_status=%s | actuator_state=%s",
             TOPIC_SENSOR, TOPIC_STATUS_ESP32, TOPIC_ACTUATOR_STATE);
    ESP_LOGI(TAG, "Topics v2 | auto_pump=%s | direct_pump=%s | direct_light=%s | planting=%s",
             TOPIC_CMD_PUMP_AUTO, TOPIC_CMD_PUMP_DIRECT, TOPIC_CMD_LIGHT_DIRECT,
             TOPIC_CMD_PLANTING_START);

    esp_err_t nvs_ret = nvs_flash_init();
    if (nvs_ret == ESP_ERR_NVS_NO_FREE_PAGES || nvs_ret == ESP_ERR_NVS_NEW_VERSION_FOUND) {
        nvs_flash_erase();
        nvs_flash_init();
    }

    s_sensor_mutex = xSemaphoreCreateMutex();
    s_i2c_mutex = xSemaphoreCreateMutex();
    setenv("TZ", TZ_INFO_UTC7, 1);
    tzset();
    ring_init();
    hw_init();
    wifi_init();

    /* BOOT FLOW chuẩn v2.9.3:
     * 1) Nếu WiFi có IP thì check NTP và cập nhật DS3231 nếu lệch.
     * 2) Load planting_start_epoch từ NVS nếu có.
     * 3) Nếu NVS rỗng: valid=false, KHÔNG tự tạo mốc mới. Gateway sẽ sync từ DB/Unity.
     * 4) Task NTP sau đó chỉ sync DS3231 định kỳ, KHÔNG ghi lại mốc gieo.
     */
    EventBits_t boot_wifi_bits = xEventGroupGetBits(s_wifi_eg);
    if (boot_wifi_bits & WIFI_CONNECTED_BIT) {
        ntp_start_once();
        if (ntp_wait_time_valid(NTP_SYNC_TIMEOUT_MS)) {
            rtc_sync_from_ntp_if_needed();
        }
    } else {
        ESP_LOGW(TAG, "BOOT: chưa có WiFi, bỏ qua NTP ban đầu; dùng DS3231 hiện có để xét planting_start");
    }
    planting_start_init_from_nvs_or_rtc();

    mqtt_init();

    /* Core pinning:
     * CORE_NET: MQTT publish + NTP
     * CORE_APP: Sensor + RTC phase/light + actuator safety
     */
    xTaskCreatePinnedToCore(ntp_rtc_sync_task,   "ntp_rtc", 4096, NULL, 3, NULL, CORE_NET);
    xTaskCreatePinnedToCore(publish_task,        "publish",  4096, NULL, 3, NULL, CORE_NET);

    xTaskCreatePinnedToCore(sensor_task,         "sensor",   4096, NULL, 5, NULL, CORE_APP);
    xTaskCreatePinnedToCore(light_schedule_task, "light",    3072, NULL, 4, NULL, CORE_APP);
    xTaskCreatePinnedToCore(actuator_state_task, "act_state",4096, NULL, 4, NULL, CORE_APP);

    ESP_LOGI(TAG, "Tất cả tasks đã khởi động (RTC phase owner + Multi-WiFi + DT direct actuator)");
}

/**
 * @file main.c
 * @brief ESP32 Firmware - Trạm Quan Trắc Buồng Ươm Giá Đỗ (Darkness Monitor)
 * Sensors: DHT11 (Có lọc nhiễu Moving Average), BH1750, ADS1115
 * Actuator: Relay (máy bơm nước)
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/event_groups.h"
#include "freertos/semphr.h"

#include "esp_system.h"
#include "esp_wifi.h"
#include "esp_event.h"
#include "esp_log.h"
#include "nvs_flash.h"
#include "esp_timer.h"

#include "driver/i2c.h"
#include "driver/gpio.h"

#include "mqtt_client.h"
#include "i2cdev.h"  

/* ---------- Component headers ---------- */
#include <ads111x.h> 
#include "bh1750.h"
#include "esp32-dht11.h"

/* ===================== CẤU HÌNH HỆ THỐNG ===================== */
#define NODE_ID             "BEAN_SPROUT_01"
#define ZONE_ID             "DARK_ROOM_A"

/* --- WiFi --- */
#define WIFI_SSID           "Phòng toàn trai đẹp"
#define WIFI_PASSWORD       "aicungdeptrai<3"
#define WIFI_MAX_RETRY      5

/* --- MQTT Broker --- */
#define MQTT_BROKER_URI     "mqtt://192.168.2.15"   // Cấu hình IP Raspberry Pi/BBB
#define MQTT_PORT           1883
#define MQTT_USERNAME       ""                       
#define MQTT_PASSWORD       ""

#define TOPIC_SENSOR_DATA   "cps/greenhouse/sensors"     
#define TOPIC_STATUS        "cps/greenhouse/status"      
#define TOPIC_CMD_PUMP      "cps/greenhouse/cmd/pump"    

/* --- Chân GPIO --- */
#define I2C_MASTER_SDA      21
#define I2C_MASTER_SCL      22
#define I2C_MASTER_PORT     I2C_NUM_0

#define DHT11_GPIO          16
#define RELAY_PUMP_GPIO     26    

/* --- Cấu hình ADS1115 --- */
#define ADS1115_I2C_ADDR    ADS111X_ADDR_GND  // 0x48

// Cấu hình Vi sai (Differential): Dùng 4 chân tạo thành 2 kênh
#define SOIL_SENSOR_COUNT   2     
static const ads111x_mux_t SOIL_MUX[SOIL_SENSOR_COUNT] = {
    ADS111X_MUX_0_1,   // Kênh vi sai 1 (AIN0 - AIN1)
    ADS111X_MUX_2_3,   // Kênh vi sai 2 (AIN2 - AIN3)
};

/* --- Cấu hình BH1750 --- */
#define BH1750_I2C_ADDR     BH1750_ADDR_LO    // 0x23

/* --- Ngưỡng Cảnh báo cục bộ (Hiển thị Serial) --- */
#define LIGHT_LEAK_THRESHOLD    10.0f    

/* --- Chu kỳ thu thập --- */
#define SENSOR_READ_PERIOD_MS   5000    
#define MQTT_PUBLISH_PERIOD_MS  10000   

/* ===================== BỘ LỌC TRUNG BÌNH ĐỘNG DHT11 ===================== */
#define DHT_FILTER_SIZE 5   

typedef struct {
    float temp_buf[DHT_FILTER_SIZE];
    float hum_buf[DHT_FILTER_SIZE];
    uint8_t index;
    uint8_t count;
} dht_filter_t;

static dht_filter_t dht_ma = {0};

static void dht11_moving_average(float new_t, float new_h, float *out_t, float *out_h) {
    dht_ma.temp_buf[dht_ma.index] = new_t;
    dht_ma.hum_buf[dht_ma.index]  = new_h;
    
    dht_ma.index = (dht_ma.index + 1) % DHT_FILTER_SIZE;
    if (dht_ma.count < DHT_FILTER_SIZE) dht_ma.count++;
    
    float sum_t = 0.0f, sum_h = 0.0f;
    for (int i = 0; i < dht_ma.count; i++) {
        sum_t += dht_ma.temp_buf[i];
        sum_h += dht_ma.hum_buf[i];
    }
    
    *out_t = sum_t / dht_ma.count;
    *out_h = sum_h / dht_ma.count;
}

/* ===================== BIẾN TOÀN CỤC ===================== */
static const char *TAG = "CPS_MAIN";

static EventGroupHandle_t s_wifi_event_group;
#define WIFI_CONNECTED_BIT  BIT0
#define WIFI_FAIL_BIT       BIT1

static esp_mqtt_client_handle_t mqtt_client = NULL;
static bool mqtt_connected = false;

static SemaphoreHandle_t sensor_mutex;
static i2c_dev_t ads_dev = {0};

typedef struct {
    float    temperature;
    float    humidity_air;
    float    humidity_soil[SOIL_SENSOR_COUNT]; 
    float    light_lux;                
    bool     pump_state;                
} sensor_data_t;

static sensor_data_t g_sensor_data = {0};
static int s_retry_num = 0;

/* ===================== KHAI BÁO HÀM (WIFI, MQTT, INIT) ===================== */

static void wifi_event_handler(void *arg, esp_event_base_t event_base, int32_t event_id, void *event_data) {
    if (event_base == WIFI_EVENT && event_id == WIFI_EVENT_STA_START) {
        esp_wifi_connect();
    } else if (event_base == WIFI_EVENT && event_id == WIFI_EVENT_STA_DISCONNECTED) {
        if (s_retry_num < WIFI_MAX_RETRY) {
            esp_wifi_connect();
            s_retry_num++;
        } else {
            xEventGroupSetBits(s_wifi_event_group, WIFI_FAIL_BIT);
        }
        mqtt_connected = false;
    } else if (event_base == IP_EVENT && event_id == IP_EVENT_STA_GOT_IP) {
        s_retry_num = 0;
        xEventGroupSetBits(s_wifi_event_group, WIFI_CONNECTED_BIT);
    }
}

static void wifi_init(void) {
    s_wifi_event_group = xEventGroupCreate();
    ESP_ERROR_CHECK(esp_netif_init());
    ESP_ERROR_CHECK(esp_event_loop_create_default());
    esp_netif_create_default_wifi_sta();
    wifi_init_config_t cfg = WIFI_INIT_CONFIG_DEFAULT();
    ESP_ERROR_CHECK(esp_wifi_init(&cfg));
    esp_event_handler_instance_register(WIFI_EVENT, ESP_EVENT_ANY_ID, &wifi_event_handler, NULL, NULL);
    esp_event_handler_instance_register(IP_EVENT, IP_EVENT_STA_GOT_IP, &wifi_event_handler, NULL, NULL);
    wifi_config_t wifi_config = {
        .sta = { .ssid = WIFI_SSID, .password = WIFI_PASSWORD, .threshold.authmode = WIFI_AUTH_WPA2_PSK },
    };
    ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_STA));
    ESP_ERROR_CHECK(esp_wifi_set_config(WIFI_IF_STA, &wifi_config));
    ESP_ERROR_CHECK(esp_wifi_start());
}

static void mqtt_event_handler(void *handler_args, esp_event_base_t base, int32_t event_id, void *event_data) {
    esp_mqtt_event_handle_t event = (esp_mqtt_event_handle_t)event_data;
    switch ((esp_mqtt_event_id_t)event_id) {
        case MQTT_EVENT_CONNECTED:
            mqtt_connected = true;
            // Subscribe vào Topic để nhận lệnh điều khiển máy bơm từ Server/Gateway
            esp_mqtt_client_subscribe(mqtt_client, TOPIC_CMD_PUMP, 1);
            break;
        case MQTT_EVENT_DISCONNECTED:
            mqtt_connected = false;
            break;
        case MQTT_EVENT_DATA:
            if (xSemaphoreTake(sensor_mutex, pdMS_TO_TICKS(100)) == pdTRUE) {
                if (strncmp(event->topic, TOPIC_CMD_PUMP, event->topic_len) == 0) {
                    // Cập nhật trạng thái máy bơm hoàn toàn dựa vào lệnh từ Gateway
                    if (strncmp(event->data, "ON", event->data_len) == 0) {
                        gpio_set_level(RELAY_PUMP_GPIO, 1); g_sensor_data.pump_state = true;
                    } else {
                        gpio_set_level(RELAY_PUMP_GPIO, 0); g_sensor_data.pump_state = false;
                    }
                }
                xSemaphoreGive(sensor_mutex);
            }
            break;
        default: break;
    }
}

static void mqtt_init(void) {
    esp_mqtt_client_config_t mqtt_cfg = {
        .broker.address.uri  = MQTT_BROKER_URI,
        .broker.address.port = MQTT_PORT,
        .session = { .keepalive = 60 },
    };
    mqtt_client = esp_mqtt_client_init(&mqtt_cfg);
    esp_mqtt_client_register_event(mqtt_client, ESP_EVENT_ANY_ID, mqtt_event_handler, NULL);
    esp_mqtt_client_start(mqtt_client);
}

static void i2c_master_init(void) { ESP_ERROR_CHECK(i2cdev_init()); }
static void relay_init(void) {
    gpio_config_t io_conf = {
        .pin_bit_mask = (1ULL << RELAY_PUMP_GPIO),
        .mode = GPIO_MODE_OUTPUT, .pull_down_en = GPIO_PULLDOWN_ENABLE,
    };
    gpio_config(&io_conf);
    gpio_set_level(RELAY_PUMP_GPIO, 0); 
}

static float ads1115_to_soil_percent(double voltage) {
    const float V_DRY = 3.3f; 
    const float V_WET = 1.2f; 
    if (voltage >= V_DRY) return 0.0f;
    if (voltage <= V_WET) return 100.0f;
    return (V_DRY - (float)voltage) / (V_DRY - V_WET) * 100.0f;
}

/* ===================== SENSOR TASK ===================== */
static void sensor_task(void *pvParameters)
{
    dht11_t dht11 = { .dht11_pin = DHT11_GPIO, .temperature = 0.0f, .humidity = 0.0f };

    i2c_dev_t bh1750_dev = {0};
    ESP_ERROR_CHECK(bh1750_init_desc(&bh1750_dev, BH1750_I2C_ADDR, I2C_MASTER_PORT, I2C_MASTER_SDA, I2C_MASTER_SCL));
    ESP_ERROR_CHECK(bh1750_power_on(&bh1750_dev));
    ESP_ERROR_CHECK(bh1750_setup(&bh1750_dev, BH1750_MODE_CONTINUOUS, BH1750_RES_HIGH2)); 

    ESP_ERROR_CHECK(ads111x_init_desc(&ads_dev, ADS1115_I2C_ADDR, I2C_MASTER_PORT, I2C_MASTER_SDA, I2C_MASTER_SCL));
    // Chế độ Continuous và Data Rate 8 SPS
    ESP_ERROR_CHECK(ads111x_set_mode(&ads_dev, ADS111X_MODE_CONTINUOUS));
    ESP_ERROR_CHECK(ads111x_set_data_rate(&ads_dev, ADS111X_DATA_RATE_8)); 
    ESP_ERROR_CHECK(ads111x_set_gain(&ads_dev, ADS111X_GAIN_4V096)); 

    uint16_t bh_raw_level = 0;
    float    real_lux = 0.0f;
    int16_t  raw_soil_val = 0;
    double   soil_v[SOIL_SENSOR_COUNT] = {0.0};
    float    soil_pct[SOIL_SENSOR_COUNT] = {0.0f};

    float filtered_temp = 0.0f;
    float filtered_hum = 0.0f;

    vTaskDelay(pdMS_TO_TICKS(2000)); 

    while (1) {
        /* Đọc và Lọc tín hiệu DHT11 */
        int dht_ret = dht11_read(&dht11, 3);
        if (dht_ret == 0) {
            dht11_moving_average(dht11.temperature, dht11.humidity, &filtered_temp, &filtered_hum);
        }
        
        /* Đọc BH1750 */
        esp_err_t bh_ret = bh1750_read(&bh1750_dev, &bh_raw_level);
        if (bh_ret == ESP_OK) {
            real_lux = (float)bh_raw_level / 2.0f; 
        }

        /* Đọc ADS1115 (Chế độ Continuous + Vi sai) */
        for (int i = 0; i < SOIL_SENSOR_COUNT; i++) {
            ads111x_set_input_mux(&ads_dev, SOIL_MUX[i]);
            
            // Vì đang ở chế độ 8 SPS (125ms/mẫu), cần delay đợi ADC hoàn thành quá trình 
            // lấy mẫu Oversampling cho kênh vi sai vừa được switch (an toàn: 150ms)
            vTaskDelay(pdMS_TO_TICKS(150));

            if (ads111x_get_value(&ads_dev, &raw_soil_val) == ESP_OK) { 
                soil_v[i] = (double)raw_soil_val * 4.096 / 32767.0;
                soil_pct[i] = ads1115_to_soil_percent(soil_v[i]);
            }
        }

        printf("\n\n================ KẾT QUẢ ĐO ĐẠC ================\n");
        if (dht_ret == 0) {
            printf("Nhiệt độ phòng ươm : %.1f °C (Đã lọc nhiễu)\n", filtered_temp);
            printf("Độ ẩm phòng ươm    : %.1f %% (Đã lọc nhiễu)\n", filtered_hum);
        } else {
            printf("DHT11              : [LỖI ĐỌC DỮ LIỆU]\n");
        }

        if (bh_ret == ESP_OK) {
            printf("Cường độ ánh sáng  : %.2f Lux\n", real_lux);
            if (real_lux > LIGHT_LEAK_THRESHOLD) {
                printf("  ---> [!!!] CẢNH BÁO KHẨN CẤP: LỌT SÁNG!\n");
            } else {
                printf("  ---> [OK] Buồng ươm tối an toàn.\n");
            }
        } else {
            printf("BH1750             : [LỖI ĐỌC DỮ LIỆU]\n");
        }

        printf("------------------------------------------------\n");
        for (int i = 0; i < SOIL_SENSOR_COUNT; i++) {
            printf("Cảm biến Đất Vi sai CH%d : %.3f V ---> %.1f %%\n", i, soil_v[i], soil_pct[i]);
        }
        printf("================================================\n\n");

        if (xSemaphoreTake(sensor_mutex, pdMS_TO_TICKS(200)) == pdTRUE) {
            if (dht_ret == 0) { 
                g_sensor_data.temperature = filtered_temp; 
                g_sensor_data.humidity_air = filtered_hum; 
            }
            if (bh_ret == ESP_OK) g_sensor_data.light_lux = real_lux;
            for (int i = 0; i < SOIL_SENSOR_COUNT; i++) g_sensor_data.humidity_soil[i] = soil_pct[i];
            
            xSemaphoreGive(sensor_mutex);
        }
        
        vTaskDelay(pdMS_TO_TICKS(SENSOR_READ_PERIOD_MS));
    }
}

/* ===================== PUBLISH MQTT TASK ===================== */
static void publish_sensor_json(void)
{
    char json_buf[350];
    sensor_data_t snapshot;

    if (xSemaphoreTake(sensor_mutex, pdMS_TO_TICKS(200)) == pdTRUE) {
        snapshot = g_sensor_data;
        xSemaphoreGive(sensor_mutex);
    } else return;

    int len = snprintf(json_buf, sizeof(json_buf),
        "{\"node\":\"%s\",\"temp\":%.1f,\"hum\":%.1f,\"s1\":%.1f,\"s2\":%.1f,\"lux\":%.2f,\"pump\":%d}",
        NODE_ID, snapshot.temperature, snapshot.humidity_air,
        snapshot.humidity_soil[0], snapshot.humidity_soil[1],
        snapshot.light_lux, snapshot.pump_state ? 1 : 0
    );

    if (len > 0 && mqtt_connected) {
        esp_mqtt_client_publish(mqtt_client, TOPIC_SENSOR_DATA, json_buf, len, 1, 0); 
    }
}

static void mqtt_publish_task(void *pvParameters)
{
    while (1) {
        publish_sensor_json();
        vTaskDelay(pdMS_TO_TICKS(MQTT_PUBLISH_PERIOD_MS));
    }
}

/* ===================== APP MAIN ===================== */
void app_main(void)
{
    ESP_LOGI(TAG, "Khởi động Node Trạm Quan Trắc Buồng Ươm Giá Đỗ");

    esp_err_t ret = nvs_flash_init();
    if (ret == ESP_ERR_NVS_NO_FREE_PAGES || ret == ESP_ERR_NVS_NEW_VERSION_FOUND) {
        ESP_ERROR_CHECK(nvs_flash_erase());
        nvs_flash_init();
    }

    sensor_mutex = xSemaphoreCreateMutex();
    i2c_master_init();
    relay_init();
    wifi_init();
    mqtt_init();

    xTaskCreate(sensor_task, "sensor_task", 4096, NULL, 5, NULL);
    xTaskCreate(mqtt_publish_task, "mqtt_pub_task", 4096, NULL, 3, NULL);
}
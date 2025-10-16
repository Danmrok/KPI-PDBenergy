#!/usr/bin/env python3

import json
import random
import datetime
from typing import Dict, List

class HydroDataGenerator:
    
    def __init__(self):
        self.turbine_types = ["kaplan", "francis", "pelton"]
        self.statuses = ["generating", "standby", "maintenance"]
        
       
        self.locations = [
            (random.uniform(47.5, 49.0), random.uniform(33.0, 36.0)) for _ in range(4)
        ]
    
    def generate_record(self, device_num: int) -> Dict:

        device_id = f"HYDRO_DN_{device_num:03d}"
        
        base_power = 50.0 + (device_num % 15) * 10.0 
        power_variation = random.uniform(-5.0, 5.0)
        power_output = base_power + power_variation
        
        efficiency = 85.0 + random.uniform(0, 10.0)
        
        day_of_year = datetime.datetime.now().timetuple().tm_yday
        temp_seasonal = 12.0 + 8.0 * abs((day_of_year - 180) / 180)
        temperature = temp_seasonal + random.uniform(-2.0, 2.0)
        
        voltage = 15000.0 + random.uniform(0, 1000.0)
        current = (power_output * 1000) / voltage 
        
        status = random.choices(
            self.statuses, 
            weights=[0.85, 0.10, 0.05]
        )[0]
        
        lat = round(random.uniform(47.5, 49.0), 4)
        lon = round(random.uniform(33.0, 36.0), 4)
        
        maintenance_hours = random.randint(500, 6000)
                
        hour = datetime.datetime.now().hour
        daily_cycle = 1.0 + 0.3 * abs((hour - 12) / 12)  # пік вдень
        base_flow = 1000.0 + (device_num % 5) * 400.0
        water_flow = base_flow * daily_cycle + random.uniform(-100, 100)
        
        base_level = 15.0 + (device_num % 10)
        water_level = base_level + random.uniform(-0.5, 0.5)
        
        turbine_type = self.turbine_types[device_num % 3]
        
        record = {
            "device_id": device_id,
            "timestamp": datetime.datetime.now().isoformat() + "Z",
            "power_output": round(power_output, 1),
            "efficiency": round(efficiency, 1),
            "temperature": round(temperature, 1),
            "voltage": round(voltage, 1),
            "current": round(current, 1),
            "status": status,
            "location": {
                "lat": round(lat, 4),
                "lon": round(lon, 4)
            },
            "maintenance_hours": maintenance_hours,
            
            "water_flow": round(water_flow, 1),
            "water_level": round(water_level, 1),
            "turbine_type": turbine_type,
            
            "reserved": "padding" + "x" * 30
        }
        
        return record
    
    def generate_batch(self, count: int) -> List[Dict]:
        return [self.generate_record(i % 15 + 1) for i in range(count)]
    
    def save_to_file(self, records: List[Dict], filename: str):
        with open(filename, 'w', encoding='utf-8') as f:
            for record in records:
                f.write(json.dumps(record, ensure_ascii=False) + '\n')
        print(f"✓ Збережено {len(records)} записів у {filename}")
    
    def print_sample(self, record: Dict):
        print("\n" + "="*60)
        print("ПРИКЛАД ЗГЕНЕРОВАНОГО ЗАПИСУ ГЕС:")
        print("="*60)
        print(json.dumps(record, indent=2, ensure_ascii=False))
        print("="*60)
        print(f"Розмір: {len(json.dumps(record))} байт")
        print("="*60 + "\n")


def main():
    gen = HydroDataGenerator()
    
    print("\n" + "🏗️ " * 20)
    print("ГЕНЕРАТОР ДАНИХ: Гідроелектростанції Дніпровського каскаду")
    print("Варіант 3, Підваріант B (Analytics Focus)")
    print("🏗️ " * 20 + "\n")
    
    datasets = {
        "hydro_test_1000.json": 1000,
    }
    
    for filename, count in datasets.items():
        print(f"📊 Генерація {count} записів...")
        records = gen.generate_batch(count)
        gen.save_to_file(records, filename)
    
    sample = gen.generate_record(1)
    gen.print_sample(sample)
    
    print("\n📈 АНАЛІТИЧНА СТАТИСТИКА:")
    print(f"├─ Загальна кількість: {sum(datasets.values())} записів")
    print(f"├─ 15 гідроагрегатів × 30 сек інтервал = 0.5 rec/sec")
    print(f"└─ Compression potential: ВИСОКИЙ (стабільні паттерни)\n")
    
    print("✅ Генерація завершена успішно!\n")


if __name__ == "__main__":
    main()
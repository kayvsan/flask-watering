import numpy as np
import skfuzzy as fuzz
from skfuzzy import control as ctrl

class FuzzyWateringSystem:
    def __init__(self):
        self.setup_fuzzy_system()
    
    def setup_fuzzy_system(self):
        # Input variables
        self.soil = ctrl.Antecedent(np.arange(0, 101, 1), 'soil_moisture')
        self.air = ctrl.Antecedent(np.arange(0, 101, 1), 'air_humidity')
        self.temp = ctrl.Antecedent(np.arange(0, 41, 1), 'temperature')
        
        # Output variable in milliseconds (0-120000)
        self.watering = ctrl.Consequent(np.arange(0, 120001, 1), 'watering_time_ms')
        
        # Membership functions
        self._setup_membership_functions()
        self._setup_rules()
        
        self.system = ctrl.ControlSystem(self.rules)
        self.simulator = ctrl.ControlSystemSimulation(self.system)
    
    def _setup_membership_functions(self):
        # Soil moisture (0-100%)
        self.soil['very_dry'] = fuzz.trimf(self.soil.universe, [0, 0, 30])
        self.soil['dry'] = fuzz.trimf(self.soil.universe, [20, 35, 50])
        self.soil['moist'] = fuzz.trimf(self.soil.universe, [40, 55, 70])
        self.soil['wet'] = fuzz.trimf(self.soil.universe, [60, 100, 100])
        
        # Air humidity (0-100%)
        self.air['low'] = fuzz.trimf(self.air.universe, [0, 0, 40])
        self.air['medium'] = fuzz.trimf(self.air.universe, [30, 50, 70])
        self.air['high'] = fuzz.trimf(self.air.universe, [60, 100, 100])
        
        # Temperature (0-40°C)
        self.temp['cool'] = fuzz.trimf(self.temp.universe, [0, 0, 20])
        self.temp['warm'] = fuzz.trimf(self.temp.universe, [15, 25, 30])
        self.temp['hot'] = fuzz.trimf(self.temp.universe, [25, 40, 40])
        
        # Watering time in milliseconds (0-120000)
        self.watering['no_water'] = fuzz.trimf(self.watering.universe, [0, 0, 0])
        self.watering['very_short'] = fuzz.trimf(self.watering.universe, [0, 15000, 30000])
        self.watering['short'] = fuzz.trimf(self.watering.universe, [20000, 40000, 60000])
        self.watering['medium'] = fuzz.trimf(self.watering.universe, [50000, 70000, 90000])
        self.watering['long'] = fuzz.trimf(self.watering.universe, [80000, 120000, 120000])
    
    def _setup_rules(self):
        self.rules = [
            ctrl.Rule(self.soil['wet'], self.watering['no_water']),
            ctrl.Rule(self.soil['very_dry'], self.watering['long']),
            ctrl.Rule(self.soil['dry'] & self.temp['hot'], self.watering['long']),
            ctrl.Rule(self.soil['dry'] & self.air['high'], self.watering['medium']),
            ctrl.Rule(self.soil['moist'] & self.temp['warm'], self.watering['short']),
            ctrl.Rule(self.temp['cool'], self.watering['very_short']),
            ctrl.Rule(self.air['low'] & self.temp['hot'], self.watering['medium']),
            ctrl.Rule(self.air['high'] & self.temp['warm'], self.watering['short'])
        ]
    
    def calculate_watering(self, soil, air, temp):
        self.simulator.input['soil_moisture'] = soil
        self.simulator.input['air_humidity'] = air
        self.simulator.input['temperature'] = temp
        
        try:
            self.simulator.compute()
            duration_ms = int(round(self.simulator.output['watering_time_ms']))
            return {
                'duration_ms': duration_ms,
                'duration_seconds': duration_ms // 1000,
                'status': self._get_status(duration_ms)
            }
        except:
            return {'error': 'Invalid input range'}

    def _get_status(self, time_ms):
        if time_ms == 0: return "No watering needed (soil is wet)"
        elif time_ms <= 15000: return "Very short watering (cooling)"
        elif time_ms <= 40000: return "Short watering"
        elif time_ms <= 70000: return "Medium watering"
        else: return "Long watering (very dry soil)"
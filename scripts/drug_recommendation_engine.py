"""
Drug Recommendation Engine for Real-time Kafka Stream Processing
Extracted from sql-spark.ipynb for production use
"""
import re
from typing import Dict, List, Optional, Any


class DrugRecommendationEngine:
    """
    Real-time drug recommendation engine based on vital signs abnormalities.
    This is a simplified version that works without Spark for real-time inference.
    """
    
    # Item ID to vital sign mapping (from MIMIC-IV)
    ITEMID_MAPPING = {
        220045: 'heart_rate',
        220050: 'systolic_bp',
        220051: 'diastolic_bp',
        220210: 'respiratory_rate',
        220277: 'oxygen_saturation',
        223762: 'temperature'
    }
    
    # Clinical thresholds for abnormality detection
    CLINICAL_THRESHOLDS = {
        'heart_rate': {'min': 60, 'max': 100},
        'systolic_bp': {'min': 90, 'max': 140},
        'diastolic_bp': {'min': 60, 'max': 90},
        'oxygen_saturation': {'min': 90, 'max': 100},
        'respiratory_rate': {'min': 12, 'max': 20},
        'temperature': {'min': 36.0, 'max': 37.8}
    }
    
    # Therapeutic drugs to recommend (excluding basic IV fluids)
    THERAPEUTIC_DRUGS = [
        'Furosemide', 'Metoprolol Tartrate', 'Insulin', 
        'Heparin', 'Vancomycin', 'Potassium Chloride',
        'Magnesium Sulfate', 'Calcium Gluconate'
    ]
    
    def __init__(self):
        """Initialize the recommendation engine"""
        self.patient_state = {}  # Track patient vital signs over time
        
    def is_abnormal(self, vital_name: str, value: float) -> bool:
        """
        Check if a vital sign value is abnormal
        
        Args:
            vital_name: Name of the vital sign
            value: Numeric value
            
        Returns:
            True if abnormal, False otherwise
        """
        if vital_name not in self.CLINICAL_THRESHOLDS:
            return False
            
        thresholds = self.CLINICAL_THRESHOLDS[vital_name]
        
        if vital_name == 'oxygen_saturation':
            return value < thresholds['min']
        else:
            return value < thresholds['min'] or value > thresholds['max']
    
    def process_kafka_message(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Process a single Kafka message and detect abnormalities
        
        Args:
            message: Kafka message with chartevent data
            
        Returns:
            Dict with abnormality info and recommendations, or None if no abnormality
        """
        itemid = message.get('itemid')
        valuenum = message.get('valuenum')
        subject_id = message.get('subject_id')
        hadm_id = message.get('hadm_id')
        
        # Skip if missing critical fields
        if itemid is None or valuenum is None:
            return None
        
        # Map itemid to vital sign name
        vital_name = self.ITEMID_MAPPING.get(itemid)
        if not vital_name:
            return None
        
        # Check for abnormality
        if not self.is_abnormal(vital_name, float(valuenum)):
            return None
        
        # Build vital signs dict for this patient
        patient_key = f"{subject_id}_{hadm_id}"
        if patient_key not in self.patient_state:
            self.patient_state[patient_key] = {
                'subject_id': subject_id,
                'hadm_id': hadm_id,
                'vital_signs': {},
                'abnormalities': []
            }
        
        # Update vital signs
        self.patient_state[patient_key]['vital_signs'][vital_name] = float(valuenum)
        self.patient_state[patient_key]['abnormalities'].append({
            'vital_name': vital_name,
            'value': float(valuenum),
            'itemid': itemid,
            'charttime': message.get('charttime'),
            'event_time': message.get('event_time')
        })
        
        # Get current vital signs state
        vital_signs = self.patient_state[patient_key]['vital_signs']
        
        # Generate recommendations
        recommendations = self._generate_recommendations(vital_signs)
        
        if recommendations:
            return {
                'subject_id': subject_id,
                'hadm_id': hadm_id,
                'abnormal_vital': vital_name,
                'abnormal_value': float(valuenum),
                'all_vital_signs': vital_signs.copy(),
                'abnormalities_count': len(self.patient_state[patient_key]['abnormalities']),
                'recommendations': recommendations,
                'event_time': message.get('event_time'),
                'charttime': message.get('charttime')
            }
        
        return None
    
    def _generate_recommendations(self, vital_signs: Dict[str, float]) -> List[Dict[str, Any]]:
        """
        Generate drug recommendations based on vital signs
        
        Args:
            vital_signs: Dict of vital sign name -> value
            
        Returns:
            List of recommended drugs with rationale
        """
        recommendations = []
        
        # Count abnormalities
        abnormal_count = sum(
            1 for vital_name, value in vital_signs.items()
            if self.is_abnormal(vital_name, value)
        )
        
        if abnormal_count == 0:
            return []
        
        # Drug-specific recommendations based on abnormalities
        heart_rate = vital_signs.get('heart_rate')
        systolic_bp = vital_signs.get('systolic_bp')
        diastolic_bp = vital_signs.get('diastolic_bp')
        oxygen_sat = vital_signs.get('oxygen_saturation')
        respiratory_rate = vital_signs.get('respiratory_rate')
        temperature = vital_signs.get('temperature')
        
        # Furosemide - for hypertension/fluid overload
        if (systolic_bp and systolic_bp > 140) or (heart_rate and heart_rate > 100):
            recommendations.append({
                'drug': 'Furosemide',
                'confidence': 0.75 if systolic_bp and systolic_bp > 140 else 0.65,
                'rationale': self._get_rationale('Furosemide', vital_signs),
                'urgency': 'HIGH' if systolic_bp and systolic_bp > 160 else 'MEDIUM',
                'suggested_action': 'Consider 20-40 mg IV for fluid overload/hypertension'
            })
        
        # Metoprolol - for tachycardia/hypertension
        if heart_rate and heart_rate > 100:
            recommendations.append({
                'drug': 'Metoprolol Tartrate',
                'confidence': 0.80 if heart_rate > 120 else 0.70,
                'rationale': self._get_rationale('Metoprolol Tartrate', vital_signs),
                'urgency': 'HIGH' if heart_rate > 120 else 'MEDIUM',
                'suggested_action': 'Consider 25-50 mg PO for rate control'
            })
        
        # Vancomycin - for fever/infection
        if temperature and temperature > 38.0:
            recommendations.append({
                'drug': 'Vancomycin',
                'confidence': 0.85 if temperature > 38.5 else 0.70,
                'rationale': self._get_rationale('Vancomycin', vital_signs),
                'urgency': 'HIGH' if temperature > 38.5 else 'MEDIUM',
                'suggested_action': 'Check cultures and consider 15-20 mg/kg IV'
            })
        
        # Heparin - for immobility/clotting risk
        if abnormal_count >= 2:
            recommendations.append({
                'drug': 'Heparin',
                'confidence': 0.70,
                'rationale': self._get_rationale('Heparin', vital_signs),
                'urgency': 'MEDIUM',
                'suggested_action': 'Consider prophylactic dosing for immobility'
            })
        
        # Insulin - for hyperglycemia (if glucose available) or general ICU care
        if abnormal_count >= 2:
            recommendations.append({
                'drug': 'Insulin',
                'confidence': 0.65,
                'rationale': self._get_rationale('Insulin', vital_signs),
                'urgency': 'MEDIUM',
                'suggested_action': 'Check glucose and consider sliding scale'
            })
        
        # Electrolyte replacements
        if abnormal_count >= 2:
            # Potassium Chloride
            recommendations.append({
                'drug': 'Potassium Chloride',
                'confidence': 0.60,
                'rationale': 'Multiple abnormalities may indicate electrolyte imbalance',
                'urgency': 'MEDIUM',
                'suggested_action': 'Check electrolyte panel and replace as needed'
            })
        
        # Sort by confidence and return top 5
        recommendations.sort(key=lambda x: x['confidence'], reverse=True)
        return recommendations[:5]
    
    def _get_rationale(self, drug_name: str, vital_signs: Dict[str, float]) -> str:
        """Generate clinical rationale for drug recommendation"""
        rationales = []
        
        heart_rate = vital_signs.get('heart_rate')
        systolic_bp = vital_signs.get('systolic_bp')
        oxygen_sat = vital_signs.get('oxygen_saturation')
        respiratory_rate = vital_signs.get('respiratory_rate')
        temperature = vital_signs.get('temperature')
        
        if drug_name == 'Furosemide':
            if systolic_bp and systolic_bp > 140:
                rationales.append(f"systolic hypertension ({systolic_bp} mmHg)")
            if heart_rate and heart_rate > 100:
                rationales.append(f"tachycardia (HR: {heart_rate} bpm)")
                
        elif drug_name == 'Metoprolol Tartrate':
            if heart_rate and heart_rate > 100:
                rationales.append(f"tachycardia (HR: {heart_rate} bpm)")
            if systolic_bp and systolic_bp > 140:
                rationales.append(f"hypertension")
                
        elif drug_name == 'Vancomycin':
            if temperature and temperature > 38.0:
                rationales.append(f"fever ({temperature:.1f}°C) - possible infection")
            if oxygen_sat and oxygen_sat < 90:
                rationales.append("hypoxemia")
                
        elif drug_name == 'Heparin':
            rationales.append("multiple abnormalities - immobility risk")
            if heart_rate and heart_rate > 100:
                rationales.append("tachycardia may indicate clotting risk")
                
        elif drug_name == 'Insulin':
            rationales.append("ICU care - monitor glucose")
            if temperature and temperature > 38.0:
                rationales.append("stress hyperglycemia possible")
        
        # Add general abnormality descriptions
        if heart_rate and self.is_abnormal('heart_rate', heart_rate):
            status = "bradycardia" if heart_rate < 60 else "tachycardia"
            rationales.append(f"heart rate {heart_rate} bpm ({status})")
            
        if systolic_bp and self.is_abnormal('systolic_bp', systolic_bp):
            if systolic_bp > 140:
                rationales.append("systolic hypertension")
            elif systolic_bp < 90:
                rationales.append("systolic hypotension")
                
        if oxygen_sat and self.is_abnormal('oxygen_saturation', oxygen_sat):
            rationales.append(f"hypoxemia (SpO2: {oxygen_sat}%)")
            
        if respiratory_rate and self.is_abnormal('respiratory_rate', respiratory_rate):
            status = "bradypnea" if respiratory_rate < 12 else "tachypnea"
            rationales.append(f"respiratory rate {respiratory_rate} ({status})")
            
        if temperature and self.is_abnormal('temperature', temperature):
            status = "hypothermia" if temperature < 36.0 else "fever"
            rationales.append(f"temperature {temperature:.1f}°C ({status})")
        
        return "; ".join(rationales) if rationales else "abnormal vital signs pattern"
    
    def format_recommendation_output(self, result: Dict[str, Any]) -> str:
        """Format recommendation result as human-readable output"""
        output = []
        output.append("=" * 70)
        output.append("🚨 ABNORMAL VITAL SIGN DETECTED - DRUG RECOMMENDATIONS")
        output.append("=" * 70)
        output.append(f"Patient: Subject ID {result['subject_id']}, HADM ID {result['hadm_id']}")
        output.append(f"Abnormal Vital: {result['abnormal_vital']} = {result['abnormal_value']}")
        output.append(f"Total Abnormalities: {result['abnormalities_count']}")
        output.append(f"Event Time: {result.get('event_time', 'N/A')}")
        output.append("")
        output.append("Current Vital Signs:")
        for vital, value in result['all_vital_signs'].items():
            is_abnormal = self.is_abnormal(vital, value)
            marker = "⚠️" if is_abnormal else "✓"
            output.append(f"  {marker} {vital.replace('_', ' ').title()}: {value}")
        output.append("")
        output.append("💊 RECOMMENDED DRUGS:")
        for i, rec in enumerate(result['recommendations'], 1):
            output.append(f"  {i}. {rec['drug']} ({rec['confidence']:.0%} confidence, {rec['urgency']} urgency)")
            output.append(f"     Rationale: {rec['rationale']}")
            output.append(f"     Action: {rec['suggested_action']}")
            output.append("")
        output.append("⚠️  Disclaimer: AI suggestions require clinical validation")
        output.append("=" * 70)
        return "\n".join(output)
    
    def clear_patient_state(self, subject_id: int, hadm_id: int):
        """Clear patient state (useful for cleanup)"""
        patient_key = f"{subject_id}_{hadm_id}"
        if patient_key in self.patient_state:
            del self.patient_state[patient_key]


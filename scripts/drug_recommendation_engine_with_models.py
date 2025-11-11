"""
Drug Recommendation Engine with Trained Spark ML Models
Loads saved models from training and uses them for real-time inference
"""
import os
import json
import re
from typing import Dict, List, Optional, Any
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml import PipelineModel
from pyspark.ml.linalg import Vectors, VectorUDT


class DrugRecommendationEngineWithModels:
    """
    Drug recommendation engine that uses trained Spark ML models for inference.
    Loads models saved from the training notebook.
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
    
    def __init__(self, spark: SparkSession, model_dir: str):
        """
        Initialize the engine with trained models
        
        Args:
            spark: SparkSession instance
            model_dir: Path to the directory containing saved models
        """
        self.spark = spark
        self.model_dir = model_dir
        self.metadata = self._load_metadata()
        self.rf_models = {}
        self.lr_models = {}
        self.pipeline_model = None
        self.feature_columns = self.metadata.get('clinical_features', [])
        self.drug_vocab = self.metadata.get('drug_vocab', [])
        self.drugs = self.metadata.get('drugs', [])
        
        # Load models
        self._load_models()
        
        # Patient state tracking (for accumulating vital signs)
        self.patient_state = {}
        
    def _load_metadata(self) -> Dict:
        """Load metadata from the model directory"""
        # Handle "latest" reference
        if self.model_dir.endswith("latest") or os.path.basename(self.model_dir) == "latest":
            # Try to read from latest.txt
            model_base = os.path.dirname(self.model_dir)
            latest_txt = os.path.join(model_base, "latest.txt")
            if os.path.exists(latest_txt):
                with open(latest_txt, 'r') as f:
                    self.model_dir = f.read().strip()
            # Try symlink
            elif os.path.islink(self.model_dir):
                link_target = os.readlink(self.model_dir)
                model_base = os.path.dirname(self.model_dir)
                self.model_dir = os.path.join(model_base, link_target)
        
        metadata_path = os.path.join(self.model_dir, "metadata.json")
        if not os.path.exists(metadata_path):
            raise FileNotFoundError(f"Metadata file not found in {self.model_dir}. Please check the MODEL_DIR path.")
        
        with open(metadata_path, 'r') as f:
            return json.load(f)
    
    def _load_models(self):
        """Load all trained models from disk"""
        print(f"[engine] Loading models from {self.model_dir}")
        
        # Load pipeline model (for drug feature transformation)
        pipeline_model_path = os.path.join(self.model_dir, "pipeline_model")
        if os.path.exists(pipeline_model_path):
            try:
                self.pipeline_model = PipelineModel.load(pipeline_model_path)
                print(f"[engine] ✓ Loaded pipeline model")
            except Exception as e:
                print(f"[engine] ⚠ Failed to load pipeline model: {e}")
        
        # Load Random Forest models
        rf_models_info = self.metadata.get('rf_models', {})
        for drug, model_path in rf_models_info.items():
            try:
                # Handle relative paths - use model directory as base
                if not os.path.isabs(model_path):
                    # Extract just the directory name from the path
                    model_dir_name = os.path.basename(model_path)
                    model_path = os.path.join(self.model_dir, model_dir_name)
                else:
                    # Absolute path - use as is, but verify it exists
                    pass
                
                if os.path.exists(model_path):
                    self.rf_models[drug] = RandomForestClassificationModel.load(model_path)
                    print(f"[engine] ✓ Loaded RF model for {drug}")
                else:
                    # Try alternative path construction
                    safe_drug_name = drug.replace("/", "_").replace(" ", "_").replace("%", "pct")
                    alt_path = os.path.join(self.model_dir, f"rf_{safe_drug_name}")
                    if os.path.exists(alt_path):
                        self.rf_models[drug] = RandomForestClassificationModel.load(alt_path)
                        print(f"[engine] ✓ Loaded RF model for {drug} (from alt path)")
                    else:
                        print(f"[engine] ⚠ Model path not found: {model_path} or {alt_path}")
            except Exception as e:
                print(f"[engine] ⚠ Failed to load RF model for {drug}: {e}")
        
        print(f"[engine] Loaded {len(self.rf_models)} RF models")
        print(f"[engine] Available drugs: {list(self.rf_models.keys())}")
    
    def _create_safe_column_name(self, drug_name: str) -> str:
        """Create Spark-safe column names"""
        safe_name = re.sub(r'[^a-zA-Z0-9_]', '_', drug_name)
        safe_name = re.sub(r'_+', '_', safe_name)
        safe_name = safe_name.strip('_')
        if safe_name and safe_name[0].isdigit():
            safe_name = 'drug_' + safe_name
        return safe_name[:40]
    
    def process_vital_signs_to_features(self, vital_signs: Dict[str, float]) -> Dict[str, float]:
        """
        Convert raw vital signs to clinical features
        
        Args:
            vital_signs: Dict with vital sign values
            
        Returns:
            Dict of clinical features
        """
        features = {}
        
        # Heart rate abnormality
        hr = vital_signs.get('heart_rate')
        features['hr_abnormal'] = 1.0 if hr and (hr < 60 or hr > 100) else 0.0
        
        # Blood pressure abnormalities
        systolic = vital_signs.get('systolic_bp')
        diastolic = vital_signs.get('diastolic_bp')
        bp_abnormal = 0.0
        if systolic and (systolic < 90 or systolic > 140):
            bp_abnormal = 1.0
        if diastolic and (diastolic < 60 or diastolic > 90):
            bp_abnormal = 1.0
        features['bp_abnormal'] = bp_abnormal
        
        # Oxygen saturation
        spo2 = vital_signs.get('oxygen_saturation')
        features['spo2_abnormal'] = 1.0 if spo2 and spo2 < 90 else 0.0
        
        # Respiratory rate
        rr = vital_signs.get('respiratory_rate')
        features['rr_abnormal'] = 1.0 if rr and (rr < 12 or rr > 20) else 0.0
        
        # Temperature
        temp = vital_signs.get('temperature')
        features['temp_abnormal'] = 1.0 if temp and (temp < 36.0 or temp > 37.8) else 0.0
        
        # Aggregate features
        abnormality_list = [
            features['hr_abnormal'], features['bp_abnormal'],
            features['spo2_abnormal'], features['rr_abnormal'],
            features['temp_abnormal']
        ]
        features['total_abnormal_count'] = sum(abnormality_list)
        features['unique_abnormal_types'] = features['total_abnormal_count']
        features['abnormal_count_ratio'] = (
            features['total_abnormal_count'] / max(features['unique_abnormal_types'], 1.0)
        )
        features['abnormality_intensity'] = 0.0  # Not available in real-time
        features['multiple_abnormality_flag'] = 1.0 if features['total_abnormal_count'] >= 2.0 else 0.0
        
        return features
    
    def predict_drug_probability(self, drug: str, clinical_features: Dict, current_drugs: List[str] = None) -> float:
        """
        Predict probability of needing a drug using trained models
        
        Args:
            drug: Drug name
            clinical_features: Dict of clinical features
            current_drugs: List of currently prescribed drugs (for drug_features)
            
        Returns:
            Probability score (0-1)
        """
        if drug not in self.rf_models:
            return 0.0
        
        try:
            # Create DataFrame with clinical features
            clinical_data = [clinical_features.get(col, 0.0) for col in self.feature_columns]
            
            # Create drug features vector (simplified - in production, use pipeline_model)
            # For now, create a sparse vector indicating which drugs are present
            drug_features = [0.0] * len(self.drug_vocab)
            if current_drugs:
                for i, vocab_drug in enumerate(self.drug_vocab):
                    if any(vocab_drug.lower() in drug.lower() or drug.lower() in vocab_drug.lower() 
                           for drug in current_drugs):
                        drug_features[i] = 1.0
            
            # Combine features
            final_features = clinical_data + drug_features
            
            # Create Spark DataFrame with vector features
            # Models were trained with featuresCol="final_features"
            feature_vector = Vectors.dense(final_features)
            row = Row(final_features=feature_vector)
            schema = StructType([StructField("final_features", VectorUDT(), False)])
            df = self.spark.createDataFrame([row], schema)
            
            # Make prediction
            model = self.rf_models[drug]
            predictions = model.transform(df)
            
            # Extract probability
            prob_row = predictions.select("probability", "prediction").collect()[0]
            probability = prob_row["probability"]
            # probability is a DenseVector, get the probability of class 1 (positive class)
            if probability.size() > 1:
                return float(probability[1])
            else:
                # Binary classification: return the single probability value
                return float(probability[0])
            
        except Exception as e:
            print(f"[engine] Error predicting for {drug}: {e}")
            return 0.0
    
    def process_kafka_message(self, message: Dict) -> Optional[Dict]:
        """
        Process a single Kafka message and return recommendations if abnormalities detected
        
        Args:
            message: Kafka message with chartevent data
            
        Returns:
            Dict with recommendations or None
        """
        try:
            itemid = message.get('itemid')
            if itemid is None:
                return None
            
            # Map itemid to vital sign name
            vital_name = self.ITEMID_MAPPING.get(int(itemid))
            if not vital_name:
                return None
            
            # Get value
            valuenum = message.get('valuenum')
            if valuenum is None:
                return None
            
            valuenum = float(valuenum)
            
            # Check if abnormal
            thresholds = self.CLINICAL_THRESHOLDS.get(vital_name, {})
            is_abnormal = False
            
            if vital_name in ['heart_rate', 'systolic_bp', 'diastolic_bp', 'respiratory_rate']:
                is_abnormal = (valuenum < thresholds.get('min') or valuenum > thresholds.get('max'))
            elif vital_name == 'oxygen_saturation':
                is_abnormal = (valuenum < thresholds.get('min'))
            elif vital_name == 'temperature':
                is_abnormal = (valuenum < thresholds.get('min') or valuenum > thresholds.get('max'))
            
            if not is_abnormal:
                return None
            
            # Update patient state
            subject_id = message.get('subject_id')
            stay_id = message.get('stay_id')
            patient_key = f"{subject_id}_{stay_id}" if stay_id else str(subject_id)
            
            if patient_key not in self.patient_state:
                self.patient_state[patient_key] = {}
            
            self.patient_state[patient_key][vital_name] = valuenum
            
            # Get current patient vital signs
            patient_vitals = self.patient_state[patient_key]
            
            # Convert to features
            clinical_features = self.process_vital_signs_to_features(patient_vitals)
            
            # Generate recommendations
            recommendations = []
            for drug in self.drugs:
                if drug not in self.rf_models:
                    continue
                
                probability = self.predict_drug_probability(drug, clinical_features, [])
                
                if probability > 0.5:  # Threshold for recommendation
                    recommendations.append({
                        'drug': drug,
                        'probability': probability,
                        'confidence': f"{probability:.1%}",
                        'vital_signs': patient_vitals.copy(),
                        'abnormalities': {k: v for k, v in clinical_features.items() if k.endswith('_abnormal') and v > 0}
                    })
            
            # Sort by probability
            recommendations.sort(key=lambda x: x['probability'], reverse=True)
            
            if recommendations:
                return {
                    'subject_id': subject_id,
                    'stay_id': stay_id,
                    'timestamp': message.get('event_time'),
                    'abnormal_vital': vital_name,
                    'abnormal_value': valuenum,
                    'recommendations': recommendations[:5]  # Top 5
                }
            
            return None
            
        except Exception as e:
            print(f"[engine] Error processing message: {e}")
            return None
    
    def format_recommendation_output(self, result: Dict) -> str:
        """Format recommendation result for output"""
        lines = []
        lines.append("\n" + "="*60)
        lines.append("🚨 DRUG RECOMMENDATION ALERT")
        lines.append("="*60)
        lines.append(f"Patient: {result.get('subject_id')} (Stay: {result.get('stay_id')})")
        lines.append(f"Abnormal Vital: {result.get('abnormal_vital')} = {result.get('abnormal_value')}")
        lines.append(f"Timestamp: {result.get('timestamp')}")
        lines.append("\nRecommended Drugs:")
        
        for i, rec in enumerate(result.get('recommendations', []), 1):
            lines.append(f"  {i}. {rec['drug']} - {rec['confidence']} confidence")
        
        lines.append("="*60)
        return "\n".join(lines)


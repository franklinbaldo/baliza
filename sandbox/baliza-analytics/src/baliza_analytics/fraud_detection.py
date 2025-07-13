"""
Fraud detection algorithms for suspicious contract patterns.
"""
import pandas as pd
import numpy as np
from typing import Dict, List
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler


class ContractFraudDetector:
    """Detects suspicious patterns in government contracts."""
    
    def __init__(self):
        self.scaler = StandardScaler()
        self.isolation_forest = IsolationForest(
            contamination=0.1,  # Assume 10% anomalies
            random_state=42
        )
    
    def calculate_suspicious_score(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate suspicious score (0-100) for each contract.
        
        Returns DataFrame with additional 'score_suspeita' column.
        """
        df = df.copy()
        
        # Initialize score
        df['score_suspeita'] = 0
        
        # 1. High value contracts (20 points)
        q99_value = df['valor_global_brl'].quantile(0.99)
        df.loc[df['valor_global_brl'] > q99_value, 'score_suspeita'] += 20
        
        # 2. Unusual duration (15 points)
        median_duration = df['duracao_contrato_dias'].median()
        df.loc[df['duracao_contrato_dias'] > median_duration * 3, 'score_suspeita'] += 15
        df.loc[df['duracao_contrato_dias'] < 30, 'score_suspeita'] += 10  # Very short contracts
        
        # 3. Same supplier frequency (15 points)
        supplier_counts = df.groupby('fornecedor_ni').size()
        frequent_suppliers = supplier_counts[supplier_counts > 10].index
        very_frequent_suppliers = supplier_counts[supplier_counts > 20].index
        
        df.loc[df['fornecedor_ni'].isin(frequent_suppliers), 'score_suspeita'] += 10
        df.loc[df['fornecedor_ni'].isin(very_frequent_suppliers), 'score_suspeita'] += 15
        
        # 4. Round number values (10 points)
        round_values = (df['valor_global_brl'] % 10000 == 0) & (df['valor_global_brl'] > 100000)
        df.loc[round_values, 'score_suspeita'] += 10
        
        # 5. Weekend signatures (5 points)
        if 'data_assinatura' in df.columns:
            df['data_assinatura'] = pd.to_datetime(df['data_assinatura'])
            weekend_contracts = df['data_assinatura'].dt.dayofweek.isin([5, 6])
            df.loc[weekend_contracts, 'score_suspeita'] += 5
        
        # 6. Emergency keywords (10 points)
        if 'objeto_contrato' in df.columns:
            emergency_keywords = ['emergenc', 'urgente', 'urgênci', 'calamidade']
            emergency_pattern = '|'.join(emergency_keywords)
            emergency_contracts = df['objeto_contrato'].str.lower().str.contains(
                emergency_pattern, na=False
            )
            df.loc[emergency_contracts, 'score_suspeita'] += 10
        
        # 7. Monopoly agency-supplier (15 points)
        agency_supplier_counts = df.groupby(['orgao_cnpj', 'fornecedor_ni']).size()
        monopoly_pairs = agency_supplier_counts[agency_supplier_counts > 5].index
        for orgao, fornecedor in monopoly_pairs:
            mask = (df['orgao_cnpj'] == orgao) & (df['fornecedor_ni'] == fornecedor)
            df.loc[mask, 'score_suspeita'] += 15
        
        # Cap score at 100
        df['score_suspeita'] = np.minimum(df['score_suspeita'], 100)
        
        return df
    
    def detect_anomalies_ml(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Use machine learning to detect anomalous contracts.
        
        Returns DataFrame with additional 'anomalia_ml' column.
        """
        df = df.copy()
        
        # Select numerical features for ML
        numerical_features = [
            'valor_global_brl', 
            'duracao_contrato_dias'
        ]
        
        # Filter valid data
        valid_data = df[numerical_features].dropna()
        
        if len(valid_data) < 100:
            # Not enough data for ML
            df['anomalia_ml'] = False
            return df
        
        # Normalize features
        scaled_features = self.scaler.fit_transform(valid_data)
        
        # Detect anomalies
        anomaly_labels = self.isolation_forest.fit_predict(scaled_features)
        
        # Map back to original dataframe
        df['anomalia_ml'] = False
        df.loc[valid_data.index, 'anomalia_ml'] = (anomaly_labels == -1)
        
        return df
    
    def get_suspicious_contracts(
        self, 
        df: pd.DataFrame, 
        min_score: int = 50
    ) -> pd.DataFrame:
        """
        Get contracts with suspicious score above threshold.
        
        Args:
            df: DataFrame with contracts
            min_score: Minimum suspicious score (0-100)
            
        Returns:
            DataFrame with suspicious contracts sorted by score
        """
        df_scored = self.calculate_suspicious_score(df)
        suspicious = df_scored[df_scored['score_suspeita'] >= min_score].copy()
        
        return suspicious.sort_values('score_suspeita', ascending=False)
    
    def analyze_supplier_risk(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze risk patterns by supplier.
        
        Returns:
            DataFrame with supplier risk metrics
        """
        supplier_analysis = df.groupby(['fornecedor_ni', 'fornecedor_nome']).agg({
            'contrato_id': 'count',
            'valor_global_brl': ['sum', 'mean', 'std'],
            'orgao_cnpj': 'nunique',
            'uf_sigla': 'nunique',
            'duracao_contrato_dias': 'mean'
        }).round(2)
        
        # Flatten column names
        supplier_analysis.columns = [
            'total_contratos', 'valor_total', 'valor_medio', 'valor_std',
            'orgaos_diferentes', 'ufs_diferentes', 'duracao_media'
        ]
        
        # Calculate risk indicators
        supplier_analysis['concentracao_geografica'] = (
            supplier_analysis['ufs_diferentes'] == 1
        )
        supplier_analysis['concentracao_orgaos'] = (
            supplier_analysis['orgaos_diferentes'] <= 2
        )
        supplier_analysis['alto_volume'] = (
            supplier_analysis['total_contratos'] > supplier_analysis['total_contratos'].quantile(0.95)
        )
        
        # Risk score
        supplier_analysis['risk_score'] = (
            supplier_analysis['concentracao_geografica'].astype(int) * 20 +
            supplier_analysis['concentracao_orgaos'].astype(int) * 25 +
            supplier_analysis['alto_volume'].astype(int) * 30
        )
        
        return supplier_analysis.reset_index().sort_values('risk_score', ascending=False)
    
    def generate_fraud_report(self, df: pd.DataFrame) -> Dict:
        """
        Generate comprehensive fraud detection report.
        
        Returns:
            Dictionary with fraud analysis results
        """
        df_analyzed = self.calculate_suspicious_score(df)
        df_analyzed = self.detect_anomalies_ml(df_analyzed)
        
        total_contracts = len(df_analyzed)
        suspicious_contracts = len(df_analyzed[df_analyzed['score_suspeita'] >= 50])
        ml_anomalies = len(df_analyzed[df_analyzed['anomalia_ml']])
        
        high_risk_suppliers = self.analyze_supplier_risk(df_analyzed)
        high_risk_count = len(high_risk_suppliers[high_risk_suppliers['risk_score'] >= 50])
        
        return {
            'summary': {
                'total_contracts': total_contracts,
                'suspicious_contracts': suspicious_contracts,
                'suspicion_rate': round(suspicious_contracts / total_contracts * 100, 2),
                'ml_anomalies': ml_anomalies,
                'anomaly_rate': round(ml_anomalies / total_contracts * 100, 2),
                'high_risk_suppliers': high_risk_count
            },
            'top_suspicious': df_analyzed.nlargest(10, 'score_suspeita')[[
                'contrato_id', 'orgao_razao_social', 'fornecedor_nome', 
                'valor_global_brl', 'score_suspeita'
            ]].to_dict('records'),
            'risk_distribution': {
                'low_risk': len(df_analyzed[df_analyzed['score_suspeita'] < 25]),
                'medium_risk': len(df_analyzed[
                    (df_analyzed['score_suspeita'] >= 25) & 
                    (df_analyzed['score_suspeita'] < 50)
                ]),
                'high_risk': len(df_analyzed[df_analyzed['score_suspeita'] >= 50]),
                'critical_risk': len(df_analyzed[df_analyzed['score_suspeita'] >= 75])
            }
        }
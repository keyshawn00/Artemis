import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import accuracy_score

import re


def convert_to_bytes(size_str):
    # 定义单位和对应的字节数（1GB = 1024^3 bytes, 1MB = 1024^2 bytes, etc.）
    units = {'B': 1, 'K': 1024, 'M': 1024 ** 2, 'G': 1024 ** 3, 'T': 1024 ** 4,
             'b': 1, 'k': 1024, 'm': 1024 ** 2, 'g': 1024 ** 3, 't': 1024 ** 4}

    # 匹配数字和单位（例如 4GB、100MB）
    match = re.match(r"(\d+(\.\d+)?)\s*(\w+)", size_str.strip().lower())

    if not match:
        raise ValueError(f"Invalid size format: {size_str}")

    # 获取数字和单位
    number = float(match.group(1))
    unit = match.group(3)

    # 如果单位没有明确指定，则默认为字节（b）
    if unit not in units:
        raise ValueError(f"Unknown unit: {unit}")

    # 转换为字节数
    bytes_value = number * units[unit]
    return bytes_value



data = pd.read_csv("./dataset/Metrics1731636186346.csv")
X = data[['stageCount', 'taskCount', 'jobCount', 'duration', 'vCore', 'memory',
      'totalShuffleReadBytes', 'totalShuffleByteWritten', 'executorDeserializeTime',
      'executorDeserializeCpuTime', 'resultSerializeTime', 'memoryBytesSpill',
      'diskBytesSpill', 'inputBytesRead', 'outputBytesWritten', 'jvmGcTime', 'peakExecutionMemory']]
# print(X)
X['memory'] = X['memory'].map(convert_to_bytes)
X = X.fillna(0)
Y_type = data[['Type']]
Y_meminten = data[['Memory Intensity']]
Y_parlevel = data[['Parallel Level']]
Y_cocomplexity = data[['Code Complexity']]

def evaluate(x, y, y_name):
    y = y.values.ravel()
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.3, random_state=42)
    gbdt = GradientBoostingClassifier(n_estimators=100, learning_rate=0.1, max_depth=3, random_state=42)
    gbdt.fit(x_train, y_train)
    y_pred = gbdt.predict(x_test)
    accuracy = accuracy_score(y_test, y_pred)
    print(f"Model Accuracy for {y_name}: {accuracy:.4f}")

evaluate(X, Y_type, "type")
evaluate(X, Y_meminten, "Memory Intensity")
evaluate(X, Y_parlevel, "Parallel Level")
evaluate(X, Y_cocomplexity, "Code Complexity")

# y = np.random.randint(0, 2, size=100)
# X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)
# gbdt = GradientBoostingClassifier(n_estimators=100, learning_rate=0.1, max_depth=3, random_state=42)
# gbdt.fit(X_train, y_train)
# y_pred = gbdt.predict(X_test)
# accuracy = accuracy_score(y_test, y_pred)
# print(f"Model Accuracy: {accuracy:.4f}")
# feature_importances = gbdt.feature_importances_
# print("Feature Importances:", feature_importances)

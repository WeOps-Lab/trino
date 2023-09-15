/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.trino.plugin.datainsight;

import java.util.List;

//{
//        "error": {
//        "reason": "Invalid SQL query",
//        "details": "Query must start with SELECT, DELETE, SHOW or DESCRIBE: SELECT1 * FROM logs-2020-08-01 limit 10",
//        "type": "SQLFeatureNotSupportedException"
//        },
//        "status": 400
//        }

//{
//        "schema": [
//        {
//        "name": "value",
//        "type": "double"
//        },
//        {
//        "name": "timestamp",
//        "type": "timestamp"
//        }
//        ],
//        "datarows": [
//        [
//        12.2,
//        "2023-08-29 10:27:59"
//        ]
//        ],
//        "total": 1,
//        "size": 1,
//        "status": 200
//        }
public class PluginResult {
    public List<DataInsightColumn> schemas;
    public List<List<Object>> datarows;
    public int total;
    public int size;

    public int getSize() {
        return size;
    }

    public List<DataInsightColumn> getSchemas() {
        return schemas;
    }

    public List<List<Object>> getDatarows() {
        return datarows;
    }

    public int getTotal() {
        return total;
    }

    public void setDatarows(List<List<Object>> datarows) {
        this.datarows = datarows;
    }

    public void setSchemas(List<DataInsightColumn> schemas) {
        this.schemas = schemas;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public void setTotal(int total) {
        this.total = total;
    }

    @Override
    public String toString() {
        return "PluginResult{" +
                "schemas=" + schemas +
                ", datarows=" + datarows +
                ", total=" + total +
                ", size=" + size +
                '}';
    }
}

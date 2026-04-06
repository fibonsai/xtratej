/*
 *  Copyright (c) 2026 fibonsai.com
 *  All rights reserved.
 *
 *  This source is subject to the Apache License, Version 2.0.
 *  Please see the LICENSE file for more information.
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.fibonsai.xtratej.adapter.core.decoders;

import com.fibonsai.xtratej.event.series.dao.TimeSeries;
import tools.jackson.databind.JsonNode;

import java.sql.ResultSet;

public interface Decoder {

    String TS_ID_UNDEF = "UNDEF";

    Decoder setId(String timeseriesId);

    default TimeSeries decode(ResultSet rs) {
        throw new RuntimeException("not implemented");
    }

    default TimeSeries decode(JsonNode jsonNode) {
        throw new RuntimeException("not implemented");
    }

    default TimeSeries decode(String str) {
        throw new RuntimeException("not implemented");
    }
}

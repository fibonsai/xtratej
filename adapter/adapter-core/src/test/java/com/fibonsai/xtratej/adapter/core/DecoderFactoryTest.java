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

package com.fibonsai.xtratej.adapter.core;

import com.fibonsai.xtratej.adapter.core.ftdata.FtDataCandlestickDecoder;
import com.fibonsai.xtratej.adapter.core.ftdata.FtDataTradeDecoder;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DecoderFactoryTest {

    @Test
    void FT_DATA_CANDLESTICK_shouldReturnFtDataCandlestickDecoderInstance() {
        Decoder decoder = DecoderFactory.FT_DATA_CANDLESTICK.get();

        assertNotNull(decoder);
        assertInstanceOf(FtDataCandlestickDecoder.class, decoder);
    }

    @Test
    void FT_DATA_TRADE_shouldReturnFtDataTradeDecoderInstance() {
        Decoder decoder = DecoderFactory.FT_DATA_TRADE.get();

        assertNotNull(decoder);
        assertInstanceOf(FtDataTradeDecoder.class, decoder);
    }

    @Test
    void eachFactoryShouldReturnNewInstance() {
        Decoder decoder1 = DecoderFactory.FT_DATA_CANDLESTICK.get();
        Decoder decoder2 = DecoderFactory.FT_DATA_CANDLESTICK.get();

        assertNotNull(decoder1);
        assertNotNull(decoder2);
        assertNotSame(decoder1, decoder2);
    }

    @Test
    void eachTradeFactoryShouldReturnNewInstance() {
        Decoder decoder1 = DecoderFactory.FT_DATA_TRADE.get();
        Decoder decoder2 = DecoderFactory.FT_DATA_TRADE.get();

        assertNotNull(decoder1);
        assertNotNull(decoder2);
        assertNotSame(decoder1, decoder2);
    }

    @Test
    void getMethodShouldSupportFluentAPI() {
        Decoder decoder = DecoderFactory.FT_DATA_TRADE.get();

        assertNotNull(decoder);
        decoder.setId("test-id");
        assertEquals(Decoder.class, decoder.getClass().getInterfaces()[0]);
    }
}

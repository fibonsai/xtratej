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

import org.jspecify.annotations.Nullable;

import java.util.function.Supplier;

public enum DecoderFactory {
    FT_DATA_CANDLESTICK(FtDataCandlestickDecoder::new),
    FT_DATA_TRADE(FtDataTradeDecoder::new),
    UNDEF(null),
    ;

    private final @Nullable Supplier<Decoder> supplier;

    DecoderFactory(@Nullable Supplier<Decoder> supplier) {
        this.supplier = supplier;
    }

    public Decoder get() {
        if (supplier != null) {
            return supplier.get();
        } else {
            throw new IllegalStateException("UNDEF not implements Decoder");
        }
    }
}

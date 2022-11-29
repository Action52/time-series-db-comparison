#!/bin/bash
for file in *.txt; do
    [ -f "$file" ] || continue
    eval "influx write -b advdb -o ulb -p ns --format=lp --token Yg8fRzr2KvgaAES2lpWd_klDC3rzslt8i2FxCTupOx1P8snV85uu_XUGL74aA4Z-ftdtdtpqS2mqngyvZyosGg== -f ./$file"
done

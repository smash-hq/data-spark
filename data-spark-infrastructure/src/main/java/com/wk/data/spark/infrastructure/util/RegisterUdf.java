package com.wk.data.spark.infrastructure.util;

import com.wk.data.spark.infrastructure.util.cleaning.*;
import com.wk.data.spark.infrastructure.util.converting.FiledSplitUdf;
import com.wk.data.spark.infrastructure.util.converting.MoreColUdf;
import com.wk.data.spark.infrastructure.util.converting.RangeConvertUdf;
import com.wk.data.spark.infrastructure.util.converting.ValueConvertUdf;
import com.wk.data.spark.infrastructure.util.masking.IdCardMaskUdf;
import com.wk.data.spark.infrastructure.util.masking.NameMaskUdf;
import com.wk.data.spark.infrastructure.util.masking.PhoneNumberMaskUdf;
import com.wk.data.spark.infrastructure.util.processing.ArabicToCnUdf;
import com.wk.data.spark.infrastructure.util.processing.CnToArabicUdf;
import com.wk.data.spark.infrastructure.util.processing.GenerateIdCardUdf;
import com.wk.data.spark.infrastructure.util.quality.Id18LogicCheckUdf;
import com.wk.data.spark.infrastructure.util.quality.NormalCheckUdf;
import com.wk.data.spark.infrastructure.util.quality.TimeLogicCheckUdf;
import com.wk.data.spark.infrastructure.util.replacing.Empty2SomeUdf;
import com.wk.data.spark.infrastructure.util.replacing.Null2SomeUdf;
import com.wk.data.spark.infrastructure.util.replacing.Some2NullUdf;
import com.wk.data.spark.infrastructure.util.timing.DateTimeTransferUdf;
import com.wk.data.spark.infrastructure.util.timing.DateTransferUdf;
import com.wk.data.spark.infrastructure.util.timing.TimeTransferUdf;
import com.wk.data.spark.infrastructure.util.udf.MathUDF;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

/**
 * @Author: smash_hq
 * @Date: 2021/11/18 17:05
 * @Description: 注册自定义函数
 * @Version v1.0
 */
public class RegisterUdf {

    public static UDF1<String, Integer> getLength() {
        return str -> str == null ? 0 : str.length();
    }

    public static void udfRegister(SparkSession sparkSession) {
        sparkSession.sqlContext().udf().register("filedSplit", new FiledSplitUdf(), DataTypes.StringType);
        sparkSession.sqlContext().udf().register("filedFill", new FiledFillUdf(), DataTypes.StringType);
        sparkSession.sqlContext().udf().register("filedReplace", new FiledReplaceUdf(), DataTypes.StringType);
        sparkSession.sqlContext().udf().register("filedAppend", new FiledAppendIndexUdf(), DataTypes.StringType);
        sparkSession.sqlContext().udf().register("filedAppendChar", new FiledAppendCharUdf(), DataTypes.StringType);
        sparkSession.sqlContext().udf().register("filedDeleteByChar", new FiledDeleteByCharUdf(), DataTypes.StringType);
        sparkSession.sqlContext().udf().register("filedDeleteAmount", new FiledDeleteAmountUdf(), DataTypes.StringType);
        sparkSession.sqlContext().udf().register("filedDeleteChar", new FiledDeleteChar(), DataTypes.StringType);
        sparkSession.sqlContext().udf().register("deleteBlank", new FiledDeleteBlankUdf(), DataTypes.StringType);
        sparkSession.sqlContext().udf().register("filedCarry", new FiledCarryUdf(), DataTypes.StringType);
        sparkSession.sqlContext().udf().register("empty2Some", new Empty2SomeUdf(), DataTypes.StringType);
        sparkSession.sqlContext().udf().register("null2Some", new Null2SomeUdf(), DataTypes.StringType);
        sparkSession.sqlContext().udf().register("some2Null", new Some2NullUdf(), DataTypes.StringType);
        sparkSession.sqlContext().udf().register("generateCardId", new GenerateIdCardUdf(), DataTypes.StringType);
        sparkSession.sqlContext().udf().register("arabicToCn", new ArabicToCnUdf(), DataTypes.StringType);
        sparkSession.sqlContext().udf().register("cnToArabic", new CnToArabicUdf(), DataTypes.StringType);
        sparkSession.sqlContext().udf().register("idCardMask", new IdCardMaskUdf(), DataTypes.StringType);
        sparkSession.sqlContext().udf().register("phoneMask", new PhoneNumberMaskUdf(), DataTypes.StringType);
        sparkSession.sqlContext().udf().register("nameMask", new NameMaskUdf(), DataTypes.StringType);
        sparkSession.sqlContext().udf().register("moreCol", new MoreColUdf(), DataTypes.StringType);
        sparkSession.sqlContext().udf().register("datetimeFormat", new DateTimeTransferUdf(), DataTypes.StringType);
        sparkSession.sqlContext().udf().register("dateFormat", new DateTransferUdf(), DataTypes.StringType);
        sparkSession.sqlContext().udf().register("timeFormat", new TimeTransferUdf(), DataTypes.StringType);
        sparkSession.sqlContext().udf().register("getLength", RegisterUdf.getLength(), DataTypes.IntegerType);
        sparkSession.sqlContext().udf().register("normalCheck", new NormalCheckUdf(), DataTypes.StringType);
        sparkSession.sqlContext().udf().register("id18Logic", new Id18LogicCheckUdf(), DataTypes.StringType);
        sparkSession.sqlContext().udf().register("timeLogic", new TimeLogicCheckUdf(), DataTypes.StringType);
        sparkSession.sqlContext().udf().register("valueConvert", new ValueConvertUdf<>(), DataTypes.StringType);
        sparkSession.sqlContext().udf().register("rangeConvert", new RangeConvertUdf<>(), DataTypes.StringType);
        sparkSession.sqlContext().udf().register("subtract", MathUDF.subtract(), DataTypes.StringType);
        sparkSession.sqlContext().udf().register("subStringLR", new FiledSubStringUdf(), DataTypes.StringType);
        sparkSession.sqlContext().udf().register("assignValue", new FiledAssignValueUdf(), DataTypes.StringType);
    }
}

package com.mylnikov;

import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * UDTF to get is, device and browser names from User-Agent string
 */
public class UserAgentUDTF extends GenericUDTF {

    private PrimitiveObjectInspector stringOI = null;

    /**
     * Init method to get info about result fields.
     * @param argOIs input argument info
     * @return 3 structured string inspectors
     * @throws UDFArgumentException
     */
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        List<? extends StructField> fields = argOIs.getAllStructFieldRefs();
        if (fields.size() != 1) {
            throw new UDFArgumentException("UDF should take only one parameter");
        }
        ObjectInspector inputFieldInspector = fields.get(0).getFieldObjectInspector();
        if (inputFieldInspector.getCategory() != ObjectInspector.Category.PRIMITIVE
                && ((PrimitiveObjectInspector) inputFieldInspector).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentException("UDF should take only string parameter");
        }
        stringOI = (PrimitiveObjectInspector) inputFieldInspector;
        List<String> fieldNames = Arrays.asList("device", "os", "browser");
        List<ObjectInspector> fieldsOIs = new ArrayList<>(3);
        for (int i = 0; i < 3; i++) {
            fieldsOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        }
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldsOIs);
    }

    /**
     * Gets the device, os, browser and forward the, to hive.
     * @param args input to process, should be length of 1 string
     * @throws HiveException
     */
    @Override
    public void process(Object[] args) throws HiveException {
        String userAgentInput = stringOI.getPrimitiveJavaObject(args[0]).toString();
        UserAgent parsedUserAgent = UserAgent.parseUserAgentString(userAgentInput);
        Object[] output = new Object[] {
                parsedUserAgent.getOperatingSystem().getDeviceType().getName(),
                parsedUserAgent.getOperatingSystem().getName(),
                parsedUserAgent.getBrowser().getName()
        };
        forward(output);
    }

    @Override
    public void close() throws HiveException {

    }
}

/*******************************************************************************
 * Copyright 2018 Goods And Services Tax Network
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License.  You may obtain a copy
 * of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations under
 * the License.
 ******************************************************************************/
package org.gstn.schemaexplorer.util;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Utility class to parse data in bytes to it's corresponding data type and vice versa 
 *
 */
public class DataTypeUtil {
	private final static Map<String,Class> hiveToJavaDataTypes;
	static{
		hiveToJavaDataTypes = new HashMap<>();
		hiveToJavaDataTypes.put("boolean",Boolean.class);
		hiveToJavaDataTypes.put("tinyint",Byte.class);
		hiveToJavaDataTypes.put("smallint",Short.class);
		hiveToJavaDataTypes.put("int",Integer.class);
		hiveToJavaDataTypes.put("integer",Integer.class);
		hiveToJavaDataTypes.put("bigint",Long.class);
		hiveToJavaDataTypes.put("float",Float.class);
		hiveToJavaDataTypes.put("double",Double.class);
		hiveToJavaDataTypes.put("string",String.class);
		hiveToJavaDataTypes.put("char",Character.class);
		hiveToJavaDataTypes.put("varchar",String.class);
		hiveToJavaDataTypes.put("decimal",BigDecimal.class);
	}
	public static String parseValue(byte[] value, Class<?> valueClass) {
        if(value == null || valueClass == null){
            return null;
        }

        if(valueClass.equals(String.class)){
            return Bytes.toString(value);
        }else if(valueClass.equals(Integer.class)){
            return Integer.toString(Bytes.toInt(value));
        }else if(valueClass.equals(Double.class)){
            return Double.toString(Bytes.toDouble(value));
        }else if(valueClass.equals(Float.class)){
            return Float.toString(Bytes.toFloat(value));
        }else if(valueClass.equals(Long.class)){
            return Long.toString(Bytes.toLong(value));
        }else if(valueClass.equals(BigDecimal.class)){
            return Bytes.toBigDecimal(value).toString();
        }else if(valueClass.equals(BigInteger.class)){
            return new BigInteger(value).toString();
        }else if(valueClass.equals(Date.class)){
        	return Bytes.toString(value);
        }else if(valueClass.equals(SnappyCompressedType.class)){
            String valueString;
            try {
                valueString = SnappyCompressedType.toString(value);
            } catch (IOException e) {
                //added to handle rows for which uncompression was failing
                valueString = Bytes.toString(value);
            }
            return valueString;
        }else if(valueClass.equals(SnappyCompressedJSON.class)){
            String valueString;
            try {

                valueString = SnappyCompressedJSON.toString(value);
            } catch (IOException e) {
                //added to handle rows for which uncompression was failing
                valueString = Bytes.toString(value);
            }
            return valueString;
        }else if(valueClass.equals(JSON.class)){
        	return Bytes.toString(value);
        }else{
            return Bytes.toString(value);
        }

    }
	
	public static  byte[] parseStringToByteArray(String value, Class<?> valueClass) {
        try {
            if(valueClass.equals(Integer.class)){
                return Bytes.toBytes(Integer.parseInt(value));
            }else if(valueClass.equals(Double.class)){
                return Bytes.toBytes(Double.parseDouble(value));
            }else if(valueClass.equals(Float.class)){
                return Bytes.toBytes(Float.parseFloat(value));
            }else if(valueClass.equals(Long.class)){
                return Bytes.toBytes(Long.parseLong(value));
            }else if(valueClass.equals(BigDecimal.class)){
                return Bytes.toBytes(new BigDecimal(value));
            }else if(valueClass.equals(BigInteger.class)){
                return new BigInteger(value).toByteArray();
            }else if(valueClass.equals(SnappyCompressedType.class)){
                return SnappyCompressedType.toBytes(value);
            }else if(valueClass.equals(SnappyCompressedJSON.class)){
                return SnappyCompressedJSON.toBytes(value);
            }else if(valueClass.equals(JSON.class)){
                return Bytes.toBytes(value);
            }
        } catch (NumberFormatException | IOException e) {
            System.err.println("Error in parseStringToByteArray for value= "+value+" and Class= "+valueClass+" :	"+e);
        }
        return Bytes.toBytes(value);
    }
	
	public static Class<?> getDataTypeClassFromStringDataType(String dataType){
		Class<?> dataTypeClass;

		if (dataType.equalsIgnoreCase("Integer")) {
			dataTypeClass = Integer.class;
		}
		if (dataType.equalsIgnoreCase("Double")) {
			dataTypeClass = Double.class;
		}
		if (dataType.equalsIgnoreCase("Float")) {
			dataTypeClass = Float.class;
		}
		if (dataType.equalsIgnoreCase("Long")) {
			dataTypeClass = Long.class;
		}
		if (dataType.equalsIgnoreCase("BigDecimal")) {
			dataTypeClass = BigDecimal.class;
		}
		if (dataType.equalsIgnoreCase("BigInteger")) {
			dataTypeClass = BigInteger.class;
		}
		if (dataType.equalsIgnoreCase("SnappyCompressedType")) {
			dataTypeClass = SnappyCompressedType.class;
		}
		if (dataType.equalsIgnoreCase("SnappyCompressedJSON")) {
			dataTypeClass = SnappyCompressedJSON.class;
		} 
		if (dataType.equalsIgnoreCase("JSON")) {
			dataTypeClass = JSON.class;
		} else {
			dataTypeClass = String.class;
		}
		
		return dataTypeClass;
	}
	
	@SuppressWarnings("unchecked")
	public static <T extends Comparable<T>> T parseValueToType(String value, Class<?> valueClass) throws IOException {
        if(value==null || valueClass == null || value.equalsIgnoreCase("null")){
            return null;
        }
        
        if(valueClass.equals(String.class)){
            return (T) value;
        }else if(valueClass.equals(Integer.class)){
            return (T)Integer.valueOf((value));
        }else if(valueClass.equals(Double.class)){
            return (T)Double.valueOf(value);
        }else if(valueClass.equals(Float.class)){
            return (T)Float.valueOf(value);
        }else if(valueClass.equals(Long.class)){
            return (T)Long.valueOf(value);
        }else if(valueClass.equals(BigDecimal.class)){
            return (T)new BigDecimal(value);
        }else if(valueClass.equals(BigInteger.class)){
            return (T)new BigInteger(value);
        }else if(valueClass.equals(Boolean.class)){
            return (T)Boolean.valueOf(value);
        }else if(valueClass.equals(Byte.class)){
            return (T)Byte.valueOf(value);
        }else if(valueClass.equals(Short.class)){
            return (T)Short.valueOf(value);
        }else if(valueClass.equals(Character.class) && value.length()==1){
            return (T)Character.valueOf(value.charAt(0));
        }else if(valueClass.equals(SnappyCompressedType.class) ||
        			valueClass.equals(SnappyCompressedJSON.class) ||
        			valueClass.equals(JSON.class)){
            return (T)value;
            
        }else{
            return (T)value;
        }
    }
	
	public static Class<?> getDataTypeClassForHiveDataType(String hiveDataType){
		String hiveType = hiveDataType.toLowerCase();
		
		if (hiveToJavaDataTypes.containsKey(hiveType)) {
			return hiveToJavaDataTypes.get(hiveType);
		}else{
			return String.class;
		}
		
	}
}

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
package org.gstn.hbasetohive.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class FormatDateUtil {

	public static String FormatDate(String format, String value) throws ParseException {
		if (value == null) {
			return null;
		}
		SimpleDateFormat sdfSource = new SimpleDateFormat(format);
		Date date = null;
		date = sdfSource.parse(value);
		SimpleDateFormat sdfDestination = new SimpleDateFormat("yyyy-MM-dd");
		return sdfDestination.format(date);
	}

	public static String FormatDate(String value) throws ParseException {
		if (value == null) {
			return null;
		}
		SimpleDateFormat sdfSource = new SimpleDateFormat("dd-MM-yyyy");
		Date date = null;
		date = sdfSource.parse(value);
		SimpleDateFormat sdfDestination = new SimpleDateFormat("yyyy-MM-dd");
		return sdfDestination.format(date);
	}

}

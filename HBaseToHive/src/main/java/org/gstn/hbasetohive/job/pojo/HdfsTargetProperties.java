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
package org.gstn.hbasetohive.job.pojo;

import java.io.Serializable;

import org.gstn.hbasetohive.exception.ValidationException;

@SuppressWarnings("serial")
public class HdfsTargetProperties implements Serializable {

	private String destHdfsUrl;
	private String hdfsBasePath;

	public String getDestHdfsUrl() {
		return destHdfsUrl;
	}

	public void setDestHdfsUrl(String destHdfsUrl) {
		this.destHdfsUrl = destHdfsUrl;
	}

	public String getHdfsBasePath() {
		return hdfsBasePath;
	}

	public void setHdfsBasePath(String hdfsBasePath) {
		this.hdfsBasePath = hdfsBasePath;
	}

	public void validate(String scFilePath) throws ValidationException {
		if (destHdfsUrl == null) {
			throw new ValidationException(
					"<destHdfsUrl> missing inside <hdfsTargetProperties> in config file: " + scFilePath);
		}
		if (destHdfsUrl.isEmpty()) {
			throw new ValidationException(
					"Please specify a valid URL for <destHdfsUrl> in <hdfsTargetProperties> in config file: "
							+ scFilePath);
		}
		if (hdfsBasePath == null) {
			throw new ValidationException(
					"<hdfsBasePath> missing inside <hdfsTargetProperties> in config file: " + scFilePath);
		}
		if (hdfsBasePath.isEmpty()) {
			throw new ValidationException(
					"Please specify a valid path for <hdfsBasePath> in <hdfsTargetProperties> in config file: "
							+ scFilePath);
		}
	}

}

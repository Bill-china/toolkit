package com.qihoo.toolkit;

import org.springframework.web.multipart.commons.CommonsMultipartResolver;

public class MyCommonsMultipartResolver extends CommonsMultipartResolver {

	@Override
	protected String getDefaultEncoding() {
		String encoding = getFileUpload().getHeaderEncoding();
		if (encoding == null) {
			encoding = "UTF-8";
		}
		return encoding;
	}

}

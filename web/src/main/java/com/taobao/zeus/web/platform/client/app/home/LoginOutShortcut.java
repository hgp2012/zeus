package com.taobao.zeus.web.platform.client.app.home;

import com.taobao.zeus.web.platform.client.theme.ResourcesTool;
import com.taobao.zeus.web.platform.client.widget.Shortcut;

public class LoginOutShortcut extends Shortcut{

	public LoginOutShortcut(){
		super("loginOut", "账户退出");
		setIcon(ResourcesTool.iconResources.terminal());
	}


}

package com.taobao.zeus.web.platform.client.app.home;

import com.google.gwt.core.shared.GWT;
import com.google.gwt.user.client.ui.HasWidgets;
import com.taobao.zeus.web.platform.client.app.Application;
import com.taobao.zeus.web.platform.client.app.PlacePath;
import com.taobao.zeus.web.platform.client.app.PlatformPlace;
import com.taobao.zeus.web.platform.client.app.PlacePath.App;
import com.taobao.zeus.web.platform.client.module.filemanager.FileModel;
import com.taobao.zeus.web.platform.client.util.GWTEnvironment;
import com.taobao.zeus.web.platform.client.util.PlatformContext;
import com.taobao.zeus.web.platform.client.util.Presenter;
import com.taobao.zeus.web.platform.client.util.RPCS;
import com.taobao.zeus.web.platform.client.util.async.AbstractAsyncCallback;
import com.taobao.zeus.web.platform.client.util.template.TemplateResources;
import com.taobao.zeus.web.platform.client.widget.Shortcut;

public class LoginOutApp implements Application{

	private LoginOutShortcut shortcut=new LoginOutShortcut();
	private PlatformContext context;
	public LoginOutApp(PlatformContext context){
		this.context=context;
	}
	@Override
	public Shortcut getShortcut() {
		return shortcut;
	}

	@Override
	public PlatformContext getPlatformContext() {
		return context;
	}

	private LoginOutWidget widget=new LoginOutWidget();
	@Override
	public Presenter getPresenter() {
		return new Presenter() {
			public void go(HasWidgets hasWidgets) {
				com.google.gwt.user.client.Window.Location.assign("/zeus-web/login_out.do");
			}
			@Override
			public PlatformContext getPlatformContext() {
				return null;
			}
		};
	}
	@Override
	public PlatformPlace getPlace() {
		com.google.gwt.user.client.Window.Location.assign("/zeus-web/login_out.do");
		return new PlacePath().toApp(App.LoginOut).create();
	}

	public static final String TAG="loginOut";
}

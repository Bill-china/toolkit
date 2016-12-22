(function() {
	function Area(options) {
		this.options = options;
		this.options.prefix = "    ->";
		this.init();
	}
	Area.prototype = {
		init: function() {
			var __self__ = this;

			this.tpl().bind().render().set(this.options.bind.value);

			new Boxy(this.wrapper, {
				"title": "请选择区域",
				"unloadOnHide": true,
				"modal": true,
				"beforeUnload": function() {}
			});
		},
		tpl: function() {
			var wrapper, container, lists, btns, btn_add, btn_remove, btn_confirm, source, create = function(ele) {
				return document.createElement(ele);
			};

			this.wrapper = wrapper = create("div");
			this.container = container = create("div");
			this.source = source = create("select");
			this.btns = btns = create("span");
			this.lists = lists = create("select");
			this.btn_add = btn_add = create("input");
			this.btn_remove = btn_remove = create("input");
			this.btn_confirm = btn_confirm = create("input");

			btn_add.type = "button";
			btn_add.value = "添加>>";
			btn_remove.type = "button";
			btn_remove.value = "<<移除";
			btn_confirm.type = "button";
			btn_confirm.value = "确定";
			btn_confirm.className = "boxy-close";
			source.multiple = "multiple";
			lists.multiple = "multiple";

			//wrapper.style.cssText = "_position:absolute;";
			container.style.cssText = "overflow:hidden;";
			source.style.cssText = "height:200px; width:100px; float:left;"
			btns.style.cssText = "float:left; margin:50px 20px";
			btn_add.style.cssText = "display:block;width:60px;margin-bottom:10px";
			btn_remove.style.cssText = "display:block;width:60px";
			lists.style.cssText = "height:200px; width:100px; float:left;";
			btn_confirm.style.cssText = "display:block; clear:both; margin:10px auto;";

			btns.appendChild(btn_add);
			btns.appendChild(btn_remove);
			container.appendChild(source);
			container.appendChild(btns);
			container.appendChild(lists);
			container.appendChild(btn_confirm);
			wrapper.appendChild(container);

			return this;
		},
		render: function(data) {
			var option, __self__ = this,
			one;
			if (data) {
				this.options.data = data;
			} else {
				data = this.options.data;
			}
			this.source.length = 0; //清除source,不需要清除lists
			//this.lists.length = 0;
			for (var i = 0, l = data.length; i < l; i++) {
				if (data[i].parentareacode == 0 && ! this.__exist__(data[i])) {
					this.__addOption__(this.source, data[i]);
				}
			}
			return this;
		},
		bind: function() {
			var __self__ = this,
			data = this.options.data;
			this.btn_add.onclick = function() {
				__self__.__add__.call(__self__);
			};
			this.btn_remove.onclick = function() {
				__self__.__remove__.call(__self__);
			}
			this.btn_confirm.onclick = function() {
				//console.log(__self__.__getData__());
				__self__.options.bind.value = __self__.__getData__();
				//console.log(area);
				delete __self__;
			}
			this.source.onclick = function() {
				var index = this.selectedIndex,
				options = this.options,
				curOption = options[index];
				if (curOption.parentareacode != 0 || curOption.value == 0) {
					return false;
				}
				__self__.__clearArea__();
				for (var m = 0, n = data.length; m < n; m++) {
					if (data[m].parentareacode == curOption.value && ! __self__.__exist__(data[m])) {
						__self__.__addOption__(__self__.source, data[m], curOption.nextSibling);
					}
				}
			}
			return this;
		},
		set: function(areacodes) {
			var areas = areacodes.split(";"),
			options = this.lists.options,
			data = this.options.data;
			for (var i = areas.length - 1; i >= 0; i--) {
				for (var m = data.length - 1; m >= 0; m--) {
					if (areas[i] == data[m].areacode) {
						this.__addOption__(this.lists, data[m]);
					}
				}
			}
			this.render();
		},

		//===============================private=============================
		__exist__: function(area) {
			var options = this.lists.options;
			for (var i = 0, l = options.length; i < l; i++) {
				if (options[i].value == area.areacode) {
					return true;
				}
			}
			return false;
		},
		//obj(要添加option的select)
		//area(标准的area数据格式，JSON串)
		//新的option将插入到next节点之前,默认采用appendChild
		__addOption__: function(obj, area, next) {
			//var option = new Option(area.areaname, area.areacode), data = this.options.data, __self__ = this;
			var option = document.createElement("option"),
			data = this.options.data,
			__self__ = this,
			one,
			curOption;
			option.arealevel = area.arealevel;
			option.parentareacode = area.parentareacode;
			option.value = area.areacode;
			if (area.arealevel == 1) {
				option.style.paddingLeft = "10px";
				option.text = this.options.prefix + area.areaname;
				if (window.navigator.userAgent.toUpperCase().indexOf("MSIE") > 0) { //ie 他妈的还需要专门设置下innerText,不然是白色的
					option.innerText = this.options.prefix + area.areaname;
				}
			} else {
				option.text = area.areaname;
				if (window.navigator.userAgent.toUpperCase().indexOf("MSIE") > 0) { //ie 他妈的还需要专门设置下innerText,不然是白色的
					option.innerText = area.areaname;
				}
			}

			if (next) {
				obj.insertBefore(option, next);
			} else {
				obj.appendChild(option);
			}
		},
		//origin(标识从lists到source方向的移动)
		__exchangeOptions__: function(source, target, origin) {
			var options = source.options,
			option, origin = origin || false,
			cache;
			for (var i = options.length - 1; i >= 0; i--) {
				if (options[i].selected) {
					if (options[i].arealevel == 1) {
						if (origin) {
							option = this.__getAreaOption__(options[i].parentareacode);
							if (option) {
								target.insertBefore(options[i], option.nextSibling);
							}
						} else {
							target.appendChild(source.options[i]);
						}
					} else if (options[i].value == 0) {
						if (origin) {
							this.render();
						} else {
							target.appendChild(options[i]);
							this.__clearArea__(1);
						}
					} else {
						cache = options[i].value;
						target.appendChild(options[i]);
						this.__clearArea__(1, cache);
					}
				}
			}
		},
		__getAreaOption__: function(areacode) {
			var options = this.source.options;
			for (var i = options.length - 1; i >= 0; i--) {
				if (options[i].value == areacode) {
					return options[i];
				}
			}
			return false;
		},
		__getData__: function() {
			var options = this.lists.options,
			res = "";
			for (var i = options.length - 1; i >= 0; i--) {
				res += options[i].value + ";"
			}
			return res;
		},
		__add__: function() {
			this.__exchangeOptions__(this.source, this.lists);
		},
		__remove__: function() {
			this.__exchangeOptions__(this.lists, this.source, true);
		},
		//mode(0=>只清除source内部的城市optio,1=>清除source和Lists内部的城市option)
		__clearArea__: function(mode, parentareacode) {
			var optionsS = this.source.options,
			optionsL = this.lists.options,
			mode = mode || 0;
			if (mode == 0) {
				for (var i = optionsS.length - 1; i >= 0; i--) {
					if (optionsS[i].arealevel == 1) {
						optionsS.remove(i);
					}
				}
				return 0;
			}
			if (mode == 1) {
				if (parentareacode) {
					for (var i = optionsS.length - 1; i >= 0; i--) {
						if (optionsS[i].arealevel == 1 && optionsS[i].parentareacode == parentareacode) {
							optionsS.remove(i);
						}
					}
					for (var i = optionsL.length - 1; i >= 0; i--) {
						if (optionsL[i].arealevel == 1 && optionsL[i].parentareacode == parentareacode) {
							optionsL.remove(i);
						}
					}
				} else {
					for (var i = optionsS.length - 1; i >= 0; i--) {
						optionsS[i].value != 0 && optionsS.remove(i);
					}
					for (var i = optionsL.length - 1; i >= 0; i--) {
						optionsL[i].value != 0 && optionsL.remove(i);
					}
				}
				return 1;
			}
			return false;
		}
	}
	window.Area = Area;

	document.getElementById("selftype").onclick = function() {
		new Area({
			"data": addr_data,
			"bind": document.getElementById("area_chose")
		});
	};
})();


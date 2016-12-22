(function($) {
	$.extend({
		tablesorter: new
		function() {
			var parsers = [],
			widgets = [];
			this.defaults = {
				cssHeader: "header",
				cssAsc: "headerSortUp",
				cssDesc: "headerSortDown",
				cssChildRow: "expand-child",
				sortInitialOrder: "asc",
				sortMultiSortKey: "shiftKey",
				sortForce: null,
				sortAppend: null,
				sortLocaleCompare: true,
				textExtraction: "simple",
				parsers: {},
				widgets: [],
				widgetZebra: {
					css: ["even", "odd"]
				},
				headers: {},
				widthFixed: false,
				cancelSelection: true,
				sortList: [],
				headerList: [],
				dateFormat: "us",
				decimal: '/\.|\,/g',
				onRenderHeader: null,
				selectorHeaders: 'thead th',
				debug: false
			};
			function benchmark(s, d) {
				log(s + "," + (new Date().getTime() - d.getTime()) + "ms");
			}
			this.benchmark = benchmark;
			function log(s) {
				if (typeof console != "undefined" && typeof console.debug != "undefined") {
					console.log(s);
				} else {
					alert(s);
				}
			}
			function buildParserCache(table, $headers) {
				if (table.config.debug) {
					var parsersDebug = "";
				}
				if (table.tBodies.length == 0) return;
				var rows = table.tBodies[0].rows;
				if (rows[0]) {
					var list = [],
					cells = rows[0].cells,
					l = cells.length;
					for (var i = 0; i < l; i++) {
						var p = false;
						if ($.metadata && ($($headers[i]).metadata() && $($headers[i]).metadata().sorter)) {
							p = getParserById($($headers[i]).metadata().sorter);
						} else if ((table.config.headers[i] && table.config.headers[i].sorter)) {
							p = getParserById(table.config.headers[i].sorter);
						}
						if (!p) {
							p = detectParserForColumn(table, rows, - 1, i);
						}
						if (table.config.debug) {
							parsersDebug += "column:" + i + " parser:" + p.id + "\n";
						}
						list.push(p);
					}
				}
				if (table.config.debug) {
					log(parsersDebug);
				}
				return list;
			};
			function detectParserForColumn(table, rows, rowIndex, cellIndex) {
				var l = parsers.length,
				node = false,
				nodeValue = false,
				keepLooking = true;
				while (nodeValue == '' && keepLooking) {
					rowIndex++;
					if (rows[rowIndex]) {
						node = getNodeFromRowAndCellIndex(rows, rowIndex, cellIndex);
						nodeValue = trimAndGetNodeText(table.config, node);
						if (table.config.debug) {
							log('Checking if value was empty on row:' + rowIndex);
						}
					} else {
						keepLooking = false;
					}
				}
				for (var i = 1; i < l; i++) {
					if (parsers[i].is(nodeValue, table, node)) {
						return parsers[i];
					}
				}
				return parsers[0];
			}
			function getNodeFromRowAndCellIndex(rows, rowIndex, cellIndex) {
				return rows[rowIndex].cells[cellIndex];
			}
			function trimAndGetNodeText(config, node) {
				return $.trim(getElementText(config, node));
			}
			function getParserById(name) {
				var l = parsers.length;
				for (var i = 0; i < l; i++) {
					if (parsers[i].id.toLowerCase() == name.toLowerCase()) {
						return parsers[i];
					}
				}
				return false;
			}
			function buildCache(table) {
				if (table.config.debug) {
					var cacheTime = new Date();
				}
				var totalRows = (table.tBodies[0] && table.tBodies[0].rows.length) || 0,
				totalCells = (table.tBodies[0].rows[0] && table.tBodies[0].rows[0].cells.length) || 0,
				parsers = table.config.parsers,
				cache = {
					row: [],
					normalized: []
				};
				for (var i = 0; i < totalRows; ++i) {
					var c = $(table.tBodies[0].rows[i]),
					cols = [];
					if (c.hasClass(table.config.cssChildRow)) {
						cache.row[cache.row.length - 1] = cache.row[cache.row.length - 1].add(c);
						continue;
					}
					cache.row.push(c);
					for (var j = 0; j < totalCells; ++j) {
						cols.push(parsers[j].format(getElementText(table.config, c[0].cells[j]), table, c[0].cells[j]));
					}
					cols.push(cache.normalized.length);
					cache.normalized.push(cols);
					cols = null;
				};
				if (table.config.debug) {
					benchmark("Building cache for " + totalRows + " rows:", cacheTime);
				}
				return cache;
			};
			function getElementText(config, node) {
				var text = "";
				if (!node) return "";
				if (!config.supportsTextContent) config.supportsTextContent = node.textContent || false;
				if (config.textExtraction == "simple") {
					if (config.supportsTextContent) {
						text = node.textContent;
					} else {
						if (node.childNodes[0] && node.childNodes[0].hasChildNodes()) {
							text = node.childNodes[0].innerHTML;
						} else {
							text = node.innerHTML;
						}
					}
				} else {
					if (typeof(config.textExtraction) == "function") {
						text = config.textExtraction(node);
					} else {
						text = $(node).text();
					}
				}
				return text;
			}
			function appendToTable(table, cache) {
				if (table.config.debug) {
					var appendTime = new Date()
				}
				var c = cache,
				r = c.row,
				n = c.normalized,
				totalRows = n.length,
				checkCell = (n[0].length - 1),
				tableBody = $(table.tBodies[0]),
				rows = [];
				for (var i = 0; i < totalRows; i++) {
					var pos = n[i][checkCell];
					rows.push(r[pos]);
					if (!table.config.appender) {
						var l = r[pos].length;
						for (var j = 0; j < l; j++) {
							tableBody[0].appendChild(r[pos][j]);
						}
					}
				}
				if (table.config.appender) {
					table.config.appender(table, rows);
				}
				rows = null;
				if (table.config.debug) {
					benchmark("Rebuilt table:", appendTime);
				}
				applyWidget(table);
				setTimeout(function() {
					$(table).trigger("sortEnd");
				},
				0);
			};
			function buildHeaders(table) {
				if (table.config.debug) {
					var time = new Date();
				}
				var meta = ($.metadata) ? true: false;
				var header_index = computeTableHeaderCellIndexes(table);
				$tableHeaders = $(table.config.selectorHeaders, table).each(function(index) {
					this.column = header_index[this.parentNode.rowIndex + "-" + this.cellIndex];
					this.order = formatSortingOrder(table.config.sortInitialOrder);
					this.count = this.order;
					if (checkHeaderMetadata(this) || checkHeaderOptions(table, index)) this.sortDisabled = true;
					if (checkHeaderOptionsSortingLocked(table, index)) this.order = this.lockedOrder = checkHeaderOptionsSortingLocked(table, index);
					if (!this.sortDisabled) {
						var $th = $(this).addClass(table.config.cssHeader);
						if (table.config.onRenderHeader) table.config.onRenderHeader.apply($th);
					}
					table.config.headerList[index] = this;
				});
				if (table.config.debug) {
					benchmark("Built headers:", time);
					log($tableHeaders);
				}
				return $tableHeaders;
			};
			function computeTableHeaderCellIndexes(t) {
				var matrix = [];
				var lookup = {};
				var thead = t.getElementsByTagName('THEAD')[0];
				var trs = thead.getElementsByTagName('TR');
				for (var i = 0; i < trs.length; i++) {
					var cells = trs[i].cells;
					for (var j = 0; j < cells.length; j++) {
						var c = cells[j];
						var rowIndex = c.parentNode.rowIndex;
						var cellId = rowIndex + "-" + c.cellIndex;
						var rowSpan = c.rowSpan || 1;
						var colSpan = c.colSpan || 1
						var firstAvailCol;
						if (typeof(matrix[rowIndex]) == "undefined") {
							matrix[rowIndex] = [];
						}
						for (var k = 0; k < matrix[rowIndex].length + 1; k++) {
							if (typeof(matrix[rowIndex][k]) == "undefined") {
								firstAvailCol = k;
								break;
							}
						}
						lookup[cellId] = firstAvailCol;
						for (var k = rowIndex; k < rowIndex + rowSpan; k++) {
							if (typeof(matrix[k]) == "undefined") {
								matrix[k] = [];
							}
							var matrixrow = matrix[k];
							for (var l = firstAvailCol; l < firstAvailCol + colSpan; l++) {
								matrixrow[l] = "x";
							}
						}
					}
				}
				return lookup;
			}
			function checkCellColSpan(table, rows, row) {
				var arr = [],
				r = table.tHead.rows,
				c = r[row].cells;
				for (var i = 0; i < c.length; i++) {
					var cell = c[i];
					if (cell.colSpan > 1) {
						arr = arr.concat(checkCellColSpan(table, headerArr, row++));
					} else {
						if (table.tHead.length == 1 || (cell.rowSpan > 1 || ! r[row + 1])) {
							arr.push(cell);
						}
					}
				}
				return arr;
			};
			function checkHeaderMetadata(cell) {
				if (($.metadata) && ($(cell).metadata().sorter === false)) {
					return true;
				};
				return false;
			}
			function checkHeaderOptions(table, i) {
				if ((table.config.headers[i]) && (table.config.headers[i].sorter === false)) {
					return true;
				};
				return false;
			}
			function checkHeaderOptionsSortingLocked(table, i) {
				if ((table.config.headers[i]) && (table.config.headers[i].lockedOrder)) return table.config.headers[i].lockedOrder;
				return false;
			}
			function applyWidget(table) {
				var c = table.config.widgets;
				var l = c.length;
				for (var i = 0; i < l; i++) {
					getWidgetById(c[i]).format(table);
				}
			}
			function getWidgetById(name) {
				var l = widgets.length;
				for (var i = 0; i < l; i++) {
					if (widgets[i].id.toLowerCase() == name.toLowerCase()) {
						return widgets[i];
					}
				}
			};
			function formatSortingOrder(v) {
				if (typeof(v) != "Number") {
					return (v.toLowerCase() == "desc") ? 1: 0;
				} else {
					return (v == 1) ? 1: 0;
				}
			}
			function isValueInArray(v, a) {
				var l = a.length;
				for (var i = 0; i < l; i++) {
					if (a[i][0] == v) {
						return true;
					}
				}
				return false;
			}
			function setHeadersCss(table, $headers, list, css) {
				$headers.removeClass(css[0]).removeClass(css[1]);
				var h = [];
				$headers.each(function(offset) {
					if (!this.sortDisabled) {
						h[this.column] = $(this);
					}
				});
				var l = list.length;
				for (var i = 0; i < l; i++) {
					h[list[i][0]].addClass(css[list[i][1]]);
				}
			}
			function fixColumnWidth(table, $headers) {
				var c = table.config;
				if (c.widthFixed) {
					var colgroup = $('<colgroup>');
					$("tr:first td", table.tBodies[0]).each(function() {
						colgroup.append($('<col>').css('width', $(this).width()));
					});
					$(table).prepend(colgroup);
				};
			}
			function updateHeaderSortCount(table, sortList) {
				var c = table.config,
				l = sortList.length;
				for (var i = 0; i < l; i++) {
					var s = sortList[i],
					o = c.headerList[s[0]];
					o.count = s[1];
					o.count++;
				}
			}
			function multisort(table, sortList, cache) {
				if (table.config.debug) {
					var sortTime = new Date();
				}
				var dynamicExp = "var sortWrapper = function(a,b) {",
				l = sortList.length;
				for (var i = 0; i < l; i++) {
					var c = sortList[i][0];
					var order = sortList[i][1];
					var s = (table.config.parsers[c].type == "text") ? ((order == 0) ? makeSortFunction("text", "asc", c) : makeSortFunction("text", "desc", c)) : ((order == 0) ? makeSortFunction("numeric", "asc", c) : makeSortFunction("numeric", "desc", c));
					var e = "e" + i;
					dynamicExp += "var " + e + " = " + s;
					dynamicExp += "if(" + e + ") { return " + e + "; } ";
					dynamicExp += "else { ";
				}
				var orgOrderCol = cache.normalized[0].length - 1;
				dynamicExp += "return a[" + orgOrderCol + "]-b[" + orgOrderCol + "];";
				for (var i = 0; i < l; i++) {
					dynamicExp += "}; ";
				}
				dynamicExp += "return 0; ";
				dynamicExp += "}; ";
				if (table.config.debug) {
					benchmark("Evaling expression:" + dynamicExp, new Date());
				}
				eval(dynamicExp);
				cache.normalized.sort(sortWrapper);
				if (table.config.debug) {
					benchmark("Sorting on " + sortList.toString() + " and dir " + order + " time:", sortTime);
				}
				return cache;
			};
			function makeSortFunction(type, direction, index) {
				var a = "a[" + index + "]",
				b = "b[" + index + "]";
				if (type == 'text' && direction == 'asc') {
					return "(" + a + " == " + b + " ? 0 : (" + a + " === null ? Number.POSITIVE_INFINITY : (" + b + " === null ? Number.NEGATIVE_INFINITY : (" + a + " < " + b + ") ? -1 : 1 )));";
				} else if (type == 'text' && direction == 'desc') {
					return "(" + a + " == " + b + " ? 0 : (" + a + " === null ? Number.POSITIVE_INFINITY : (" + b + " === null ? Number.NEGATIVE_INFINITY : (" + b + " < " + a + ") ? -1 : 1 )));";
				} else if (type == 'numeric' && direction == 'asc') {
					return "(" + a + " === null && " + b + " === null) ? 0 :(" + a + " === null ? Number.POSITIVE_INFINITY : (" + b + " === null ? Number.NEGATIVE_INFINITY : " + a + " - " + b + "));";
				} else if (type == 'numeric' && direction == 'desc') {
					return "(" + a + " === null && " + b + " === null) ? 0 :(" + a + " === null ? Number.POSITIVE_INFINITY : (" + b + " === null ? Number.NEGATIVE_INFINITY : " + b + " - " + a + "));";
				}
			};
			function makeSortText(i) {
				return "((a[" + i + "] < b[" + i + "]) ? -1 : ((a[" + i + "] > b[" + i + "]) ? 1 : 0));";
			};
			function makeSortTextDesc(i) {
				return "((b[" + i + "] < a[" + i + "]) ? -1 : ((b[" + i + "] > a[" + i + "]) ? 1 : 0));";
			};
			function makeSortNumeric(i) {
				return "a[" + i + "]-b[" + i + "];";
			};
			function makeSortNumericDesc(i) {
				return "b[" + i + "]-a[" + i + "];";
			};
			function sortText(a, b) {
				if (table.config.sortLocaleCompare) return a.localeCompare(b);
				return ((a < b) ? - 1: ((a > b) ? 1: 0));
			};
			function sortTextDesc(a, b) {
				if (table.config.sortLocaleCompare) return b.localeCompare(a);
				return ((b < a) ? - 1: ((b > a) ? 1: 0));
			};
			function sortNumeric(a, b) {
				return a - b;
			};
			function sortNumericDesc(a, b) {
				return b - a;
			};
			function getCachedSortType(parsers, i) {
				return parsers[i].type;
			};
			this.construct = function(settings) {
				return this.each(function() {
					if (!this.tHead || ! this.tBodies) return;
					var $this, $document, $headers, cache, config, shiftDown = 0,
					sortOrder;
					this.config = {};
					config = $.extend(this.config, $.tablesorter.defaults, settings);
					$this = $(this);
					$.data(this, "tablesorter", config);
					$headers = buildHeaders(this);
					this.config.parsers = buildParserCache(this, $headers);
					cache = buildCache(this);
					var sortCSS = [config.cssDesc, config.cssAsc];
					fixColumnWidth(this);
					$headers.click(function(e) {
						var totalRows = ($this[0].tBodies[0] && $this[0].tBodies[0].rows.length) || 0;
						if (!this.sortDisabled && totalRows > 0) {
							$this.trigger("sortStart");
							var $cell = $(this);
							var i = this.column;
							this.order = this.count++ % 2;
							if (this.lockedOrder) this.order = this.lockedOrder;
							if (!e[config.sortMultiSortKey]) {
								config.sortList = [];
								if (config.sortForce != null) {
									var a = config.sortForce;
									for (var j = 0; j < a.length; j++) {
										if (a[j][0] != i) {
											config.sortList.push(a[j]);
										}
									}
								}
								config.sortList.push([i, this.order]);
							} else {
								if (isValueInArray(i, config.sortList)) {
									for (var j = 0; j < config.sortList.length; j++) {
										var s = config.sortList[j],
										o = config.headerList[s[0]];
										if (s[0] == i) {
											o.count = s[1];
											o.count++;
											s[1] = o.count % 2;
										}
									}
								} else {
									config.sortList.push([i, this.order]);
								}
							};
							setTimeout(function() {
								setHeadersCss($this[0], $headers, config.sortList, sortCSS);
								appendToTable($this[0], multisort($this[0], config.sortList, cache));
							},
							1);
							return false;
						}
					}).mousedown(function() {
						if (config.cancelSelection) {
							this.onselectstart = function() {
								return false
							};
							return false;
						}
					});
					$this.bind("update", function() {
						var me = this;
						setTimeout(function() {
							me.config.parsers = buildParserCache(me, $headers);
							cache = buildCache(me);
						},
						1);
					}).bind("updateCell", function(e, cell) {
						var config = this.config;
						var pos = [(cell.parentNode.rowIndex - 1), cell.cellIndex];
						cache.normalized[pos[0]][pos[1]] = config.parsers[pos[1]].format(getElementText(config, cell), cell);
					}).bind("sorton", function(e, list) {
						$(this).trigger("sortStart");
						config.sortList = list;
						var sortList = config.sortList;
						updateHeaderSortCount(this, sortList);
						setHeadersCss(this, $headers, sortList, sortCSS);
						appendToTable(this, multisort(this, sortList, cache));
					}).bind("appendCache", function() {
						appendToTable(this, cache);
					}).bind("applyWidgetId", function(e, id) {
						getWidgetById(id).format(this);
					}).bind("applyWidgets", function() {
						applyWidget(this);
					});
					if ($.metadata && ($(this).metadata() && $(this).metadata().sortlist)) {
						config.sortList = $(this).metadata().sortlist;
					}
					if (config.sortList.length > 0) {
						$this.trigger("sorton", [config.sortList]);
					}
					applyWidget(this);
				});
			};
			this.addParser = function(parser) {
				var l = parsers.length,
				a = true;
				for (var i = 0; i < l; i++) {
					if (parsers[i].id.toLowerCase() == parser.id.toLowerCase()) {
						a = false;
					}
				}
				if (a) {
					parsers.push(parser);
				};
			};
			this.addWidget = function(widget) {
				widgets.push(widget);
			};
			this.formatFloat = function(s) {
				var i = parseFloat(s);
				return (isNaN(i)) ? 0: i;
			};
			this.formatInt = function(s) {
				var i = parseInt(s);
				return (isNaN(i)) ? 0: i;
			};
			this.isDigit = function(s, config) {
				return /^[-+]?\d*$/.test($.trim(s.replace(/[,.']/g, '')));
			};
			this.clearTableBody = function(table) {
				if ($.browser.msie) {
					function empty() {
						while (this.firstChild) this.removeChild(this.firstChild);
					}
					empty.apply(table.tBodies[0]);
				} else {
					table.tBodies[0].innerHTML = "";
				}
			};
		}
	});
	$.fn.extend({
		tablesorter: $.tablesorter.construct
	});
	var ts = $.tablesorter;
	ts.addParser({
		id: "text",
		is: function(s) {
			return true;
		},
		format: function(s) {
			return $.trim(s.toLocaleLowerCase());
		},
		type: "text"
	});
	ts.addParser({
		id: "digit",
		is: function(s, table) {
			var c = table.config;
			return $.tablesorter.isDigit(s, c);
		},
		format: function(s) {
			return $.tablesorter.formatFloat(s);
		},
		type: "numeric"
	});
	ts.addParser({
		id: "currency",
		is: function(s) {
			return /^[£$€?.]/.test(s);
		},
		format: function(s) {
			return $.tablesorter.formatFloat(s.replace(new RegExp(/[£$€]/g), ""));
		},
		type: "numeric"
	});
	ts.addParser({
		id: "ipAddress",
		is: function(s) {
			return /^\d{2,3}[\.]\d{2,3}[\.]\d{2,3}[\.]\d{2,3}$/.test(s);
		},
		format: function(s) {
			var a = s.split("."),
			r = "",
			l = a.length;
			for (var i = 0; i < l; i++) {
				var item = a[i];
				if (item.length == 2) {
					r += "0" + item;
				} else {
					r += item;
				}
			}
			return $.tablesorter.formatFloat(r);
		},
		type: "numeric"
	});
	ts.addParser({
		id: "url",
		is: function(s) {
			return /^(https?|ftp|file):\/\/$/.test(s);
		},
		format: function(s) {
			return jQuery.trim(s.replace(new RegExp(/(https?|ftp|file):\/\//), ''));
		},
		type: "text"
	});
	ts.addParser({
		id: "isoDate",
		is: function(s) {
			return /^\d{4}[\/-]\d{1,2}[\/-]\d{1,2}$/.test(s);
		},
		format: function(s) {
			return $.tablesorter.formatFloat((s != "") ? new Date(s.replace(new RegExp(/-/g), "/")).getTime() : "0");
		},
		type: "numeric"
	});
	ts.addParser({
		id: "percent",
		is: function(s) {
			return /\%$/.test($.trim(s));
		},
		format: function(s) {
			return $.tablesorter.formatFloat(s.replace(new RegExp(/%/g), ""));
		},
		type: "numeric"
	});
	ts.addParser({
		id: "usLongDate",
		is: function(s) {
			return s.match(new RegExp(/^[A-Za-z]{3,10}\.? [0-9]{1,2}, ([0-9]{4}|'?[0-9]{2}) (([0-2]?[0-9]:[0-5][0-9])|([0-1]?[0-9]:[0-5][0-9]\s(AM|PM)))$/));
		},
		format: function(s) {
			return $.tablesorter.formatFloat(new Date(s).getTime());
		},
		type: "numeric"
	});
	ts.addParser({
		id: "shortDate",
		is: function(s) {
			return /\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4}/.test(s);
		},
		format: function(s, table) {
			var c = table.config;
			s = s.replace(/\-/g, "/");
			if (c.dateFormat == "us") {
				s = s.replace(/(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})/, "$3/$1/$2");
			} else if (c.dateFormat == "uk") {
				s = s.replace(/(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})/, "$3/$2/$1");
			} else if (c.dateFormat == "dd/mm/yy" || c.dateFormat == "dd-mm-yy") {
				s = s.replace(/(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2})/, "$1/$2/$3");
			}
			return $.tablesorter.formatFloat(new Date(s).getTime());
		},
		type: "numeric"
	});
	ts.addParser({
		id: "time",
		is: function(s) {
			return /^(([0-2]?[0-9]:[0-5][0-9])|([0-1]?[0-9]:[0-5][0-9]\s(am|pm)))$/.test(s);
		},
		format: function(s) {
			return $.tablesorter.formatFloat(new Date("2000/01/01 " + s).getTime());
		},
		type: "numeric"
	});
	ts.addParser({
		id: "metadata",
		is: function(s) {
			return false;
		},
		format: function(s, table, cell) {
			var c = table.config,
			p = (!c.parserMetadataName) ? 'sortValue': c.parserMetadataName;
			return $(cell).metadata()[p];
		},
		type: "numeric"
	});
	ts.addWidget({
		id: "zebra",
		format: function(table) {
			if (table.config.debug) {
				var time = new Date();
			}
			var $tr, row = - 1,
			odd;
			$("tr:visible", table.tBodies[0]).each(function(i) {
				$tr = $(this);
				if (!$tr.hasClass(table.config.cssChildRow)) row++;
				odd = (row % 2 == 0);
				$tr.removeClass(table.config.widgetZebra.css[odd ? 0: 1]).addClass(table.config.widgetZebra.css[odd ? 1: 0])
			});
			if (table.config.debug) {
				$.tablesorter.benchmark("Applying Zebra widget", time);
			}
		}
	});
})(jQuery);
(function() {
	var Dota = {
		db: [['A', '阿吖嗄腌锕'], ['Ai', '埃挨哎唉哀皑癌蔼矮艾碍爱隘捱嗳嗌嫒瑷暧砹锿霭'], ['An', '鞍氨安俺按暗岸胺案谙埯揞庵桉铵鹌黯'], ['Ang', '肮昂盎'], ['Ao', '凹敖熬翱袄傲奥懊澳坳嗷岙廒遨媪骜獒聱螯鏊鳌鏖'], ['Ba', '芭捌扒叭吧笆八疤巴拔跋靶把耙坝霸罢爸茇菝岜灞钯粑鲅魃'], ['Bai', '白柏百摆佰败拜稗捭掰'], ['Ban', '斑班搬扳般颁板版扮拌伴瓣半办绊阪坂钣瘢癍舨'], ['Bang', '邦帮梆榜膀绑棒磅镑傍谤蒡浜'], ['Beng', '蚌崩绷甭泵蹦迸嘣甏'], ['Bao', '苞胞包褒薄雹保堡饱宝抱报暴豹鲍爆曝勹葆孢煲鸨褓趵龅'], ['Bo', '剥玻菠播拨钵波博勃搏铂箔伯帛舶脖膊渤泊驳亳啵饽檗擘礴钹鹁簸跛踣'], ['Bei', '杯碑悲卑北辈背贝钡倍狈备惫焙被孛陂邶蓓呗悖碚鹎褙鐾鞴'], ['Ben', '奔苯本笨畚坌锛'], ['Bi', '逼鼻比鄙笔彼碧蓖蔽毕毙毖币庇痹闭敝弊必壁臂避陛匕俾荜荸萆薜吡哔狴庳愎滗濞弼妣婢嬖璧贲睥畀铋秕裨筚箅篦舭襞跸髀'], ['Pi', '辟坯砒霹批披劈琵毗啤脾疲皮匹痞僻屁譬丕仳陴邳郫圮埤鼙芘擗噼庀淠媲纰枇甓罴铍癖疋蚍蜱貔'], ['Bian', '鞭边编贬扁便变卞辨辩辫遍匾弁苄忭汴缏煸砭碥窆褊蝙笾鳊'], ['Biao', '标彪膘表婊骠飑飙飚镖镳瘭裱鳔'], ['Bie', '鳖憋别瘪蹩'], ['Bin', '彬斌濒滨宾摈傧豳缤玢槟殡膑镔髌鬓'], ['Bing', '兵冰柄丙秉饼炳病并禀邴摒'], ['Bu', '捕卜哺补埠不布步簿部怖卟逋瓿晡钚钸醭'], ['Ca', '擦嚓礤'], ['Cai', '猜裁材才财睬踩采彩菜蔡'], ['Can', '餐参蚕残惭惨灿骖璨粲黪'], ['Cang', '苍舱仓沧藏伧'], ['Cao', '操糙槽曹草嘈漕螬艚'], ['Ce', '厕策侧册测恻'], ['Ceng', '层蹭曾噌'], ['Cha', '插叉茬茶查碴搽察岔诧猹馇汊姹杈槎檫锸镲衩'], ['Chai', '差拆柴豺侪钗瘥虿'], ['Chan', '搀掺蝉馋谗缠铲产阐颤冁谄蒇廛忏潺澶孱羼婵骣觇禅蟾躔'], ['Chang', '昌猖场尝常长偿肠厂敞畅唱倡伥鬯苌菖徜怅惝阊娼嫦昶氅鲳'], ['Chao', '超抄钞朝嘲潮巢吵炒怊晁焯耖'], ['Che', '车扯撤掣彻澈坼砗'], ['Chen', '郴臣辰尘晨忱沉陈趁衬谌谶抻嗔宸琛榇碜龀'], ['Cheng', '撑称城橙成呈乘程惩澄诚承逞骋秤丞埕枨柽塍瞠铖铛裎蛏酲'], ['Chi', '吃痴持池迟弛驰耻齿侈尺赤翅斥炽傺墀茌叱哧啻嗤彳饬媸敕眵鸱瘛褫蚩螭笞篪豉踟魑'], ['Shi', '匙师失狮施湿诗尸虱十石拾时食蚀实识史矢使屎驶始式示士世柿事拭誓逝势是嗜噬适仕侍释饰氏市恃室视试谥埘莳蓍弑轼贳炻礻铈舐筮豕鲥鲺'], ['Chong', '充冲虫崇宠重茺忡憧铳舂艟'], ['Chou', '抽酬畴踌稠愁筹仇绸瞅丑臭俦帱惆瘳雠'], ['Chu', '初出橱厨躇锄雏滁除楚础储矗搐触处亍刍怵憷绌杵楮樗褚蜍蹰黜'], ['Chuai', '揣搋嘬膪踹'], ['Chuan', '川穿椽传船喘串舛遄氚钏舡'], ['Chuang', '疮窗床闯创怆'], ['Zhuang', '幢桩庄装妆撞壮状僮'], ['Chui', '吹炊捶锤垂陲棰槌'], ['Chun', '春椿醇唇淳纯蠢莼鹑蝽'], ['Chuo', '戳绰啜辍踔龊'], ['Ci', '疵茨磁雌辞慈瓷词此刺赐次茈祠鹚糍'], ['Cong', '聪葱囱匆从丛苁淙骢琮璁枞'], ['Cou', '凑楱辏腠'], ['Cu', '粗醋簇促卒蔟徂猝殂酢蹙蹴'], ['Cuan', '蹿篡窜汆撺爨镩'], ['Cui', '摧崔催脆瘁粹淬翠萃啐悴璀榱毳'], ['Cun', '村存寸忖皴'], ['Cuo', '磋撮搓措挫错厝嵯脞锉矬痤鹾蹉'], ['Da', '搭达答瘩打大耷哒怛妲褡笪靼鞑'], ['Dai', '呆歹傣戴带殆代贷袋待逮怠埭甙呔岱迨绐玳黛'], ['Dan', '耽担丹单郸掸胆旦氮但惮淡诞蛋儋凼萏菪啖澹宕殚赕眈疸瘅聃箪'], ['Tan', '弹坍摊贪瘫滩坛檀痰潭谭谈坦毯袒碳探叹炭郯昙忐钽锬覃'], ['Dang', '当挡党荡档谠砀裆'], ['Dao', '刀捣蹈倒岛祷导到稻悼道盗叨氘焘纛'], ['De', '德得的锝'], ['Deng', '蹬灯登等瞪凳邓噔嶝戥磴镫簦'], ['Di', '堤低滴迪敌笛狄涤嫡抵底地蒂第帝弟递缔氐籴诋谛邸坻荻嘀娣柢棣觌祗砥碲睇镝羝骶'], ['Zhai', '翟摘斋宅窄债寨砦瘵'], ['Dian', '颠掂滇碘点典靛垫电佃甸店惦奠淀殿丶阽坫巅玷钿癜癫簟踮'], ['Diao', '碉叼雕凋刁掉吊钓铞貂鲷'], ['Tiao', '调挑条迢眺跳佻苕祧窕蜩笤粜龆鲦髫'], ['Die', '跌爹碟蝶迭谍叠垤堞喋牒瓞耋鲽'], ['Ding', '丁盯叮钉顶鼎锭定订仃啶玎腚碇町疔耵酊'], ['Diu', '丢铥'], ['Dong', '东冬董懂动栋侗恫冻洞垌咚岽峒氡胨胴硐鸫'], ['Dou', '兜抖斗陡豆逗痘蔸钭窦蚪篼'], ['Du', '都督毒犊独读堵睹赌杜镀肚度渡妒芏嘟渎椟牍蠹笃髑黩'], ['Duan', '端短锻段断缎椴煅簖'], ['Dui', '堆兑队对怼憝碓镦'], ['Dun', '墩吨蹲敦顿钝盾遁沌炖砘礅盹趸'], ['Tun', '囤吞屯臀氽饨暾豚'], ['Duo', '掇哆多夺垛躲朵跺舵剁惰堕咄哚沲缍铎裰踱'], ['E', '蛾峨鹅俄额讹娥恶厄扼遏鄂饿噩谔垩苊莪萼呃愕屙婀轭腭锇锷鹗颚鳄'], ['En', '恩蒽摁嗯'], ['Er', '而儿耳尔饵洱二贰迩珥铒鸸鲕'], ['Fa', '发罚筏伐乏阀法珐垡砝'], ['Fan', '藩帆番翻樊矾钒繁凡烦反返范贩犯饭泛蕃蘩幡夂梵燔畈蹯'], ['Fang', '坊芳方肪房防妨仿访纺放邡枋钫舫鲂'], ['Fei', '菲非啡飞肥匪诽吠肺废沸费狒悱淝妃绯榧腓斐扉砩镄痱蜚篚翡霏鲱'], ['Fen', '芬酚吩氛分纷坟焚汾粉奋份忿愤粪偾瀵棼鲼鼢'], ['Feng', '丰封枫蜂峰锋风疯烽逢冯缝讽奉凤俸酆葑唪沣砜'], ['Fo', '佛'], ['Fou', '否缶'], ['Fu', '夫敷肤孵扶拂辐幅氟符伏俘服浮涪福袱弗甫抚辅俯釜斧脯腑府腐赴副覆赋复傅付阜父腹负富讣附妇缚咐匐凫郛芙芾苻茯莩菔拊呋呒幞怫滏艴孚驸绂绋桴赙祓黻黼罘稃馥蚨蜉蝠蝮麸趺跗鲋鳆'], ['Ga', '噶嘎垓尬尕尜旮钆'], ['Gai', '该改概钙盖溉丐陔戤赅'], ['Gan', '干甘杆柑竿肝赶感秆敢赣坩苷尴擀泔淦澉绀橄旰矸疳酐'], ['Gang', '冈刚钢缸肛纲岗港杠戆罡筻'], ['Gao', '篙皋高膏羔糕搞镐稿告睾诰郜藁缟槔槁杲锆'], ['Ge', '哥歌搁戈鸽胳疙割革葛格阁隔铬个各鬲仡哿圪塥嗝纥搿膈硌镉袼虼舸骼'], ['Ha', '蛤哈铪'], ['Gei', '给'], ['Gen', '根跟亘茛哏艮'], ['Geng', '耕更庚羹埂耿梗哽赓绠鲠'], ['Gong', '工攻功恭龚供躬公宫弓巩汞拱贡共珙肱蚣觥'], ['Gou', '钩勾沟苟狗垢构购够佝诟岣遘媾缑枸觏彀笱篝鞲'], ['Gu', '辜菇咕箍估沽孤姑鼓古蛊骨谷股故顾固雇嘏诂菰崮汩梏轱牯牿臌毂瞽罟钴锢鸪痼蛄酤觚鲴'], ['Gua', '刮瓜剐寡挂褂卦诖呱栝胍鸹'], ['Guai', '乖拐怪掴'], ['Guan', '棺关官冠观管馆罐惯灌贯倌莞掼涫盥鹳鳏'], ['Guang', '光广逛咣犷桄胱'], ['Gui', '瑰规圭硅归龟闺轨鬼诡癸桂柜跪贵刽匦刿庋宄妫桧炅晷皈簋鲑鳜'], ['Gun', '辊滚棍衮绲磙鲧'], ['Guo', '锅郭国果裹过馘埚呙帼崞猓椁虢聒蜾蝈'], ['Hai', '骸孩海氦亥害骇嗨胲醢'], ['Han', '酣憨邯韩含涵寒函喊罕翰撼捍旱憾悍焊汗汉邗菡撖犴瀚晗焓顸颔蚶鼾'], ['Hang', '夯杭航行沆绗颃'], ['Hao', '壕嚎豪毫郝好耗号浩貉蒿薅嗥嚆濠灏昊皓颢蚝'], ['He', '呵喝荷菏核禾和何合盒阂河涸赫褐鹤贺诃劾壑嗬阖曷盍颌蚵翮'], ['Hei', '嘿黑'], ['Hen', '痕很狠恨'], ['Heng', '哼亨横衡恒蘅珩桁'], ['Hong', '轰哄烘虹鸿洪宏弘红黉訇讧荭蕻薨闳泓'], ['Hou', '喉侯猴吼厚候后堠後逅瘊篌糇鲎骺'], ['Hu', '呼乎忽瑚壶葫胡蝴狐糊湖弧虎唬护互沪户冱唿囫岵猢怙惚浒滹琥槲轷觳烀煳戽扈祜瓠鹄鹕鹱笏醐斛鹘'], ['Hua', '花哗华猾滑画划化话骅桦砉铧'], ['Huai', '槐徊怀淮坏踝'], ['Huan', '欢环桓还缓换患唤痪豢焕涣宦幻奂擐圜獾洹浣漶寰逭缳锾鲩鬟'], ['Huang', '荒慌黄磺蝗簧皇凰惶煌晃幌恍谎隍徨湟潢遑璜肓癀蟥篁鳇'], ['Hui', '灰挥辉徽恢蛔回毁悔慧卉惠晦贿秽会烩汇讳诲绘诙茴荟蕙咴喙隳洄彗缋珲晖恚虺蟪麾'], ['Hun', '荤昏婚魂浑混诨馄阍溷'], ['Huo', '豁活伙火获或惑霍货祸劐藿攉嚯夥钬锪镬耠蠖'], ['Ji', '击圾基机畸稽积箕肌饥迹激讥鸡姬绩缉吉极棘辑籍集及急疾汲即嫉级挤几脊己蓟技冀季伎祭剂悸济寄寂计记既忌际妓继纪藉亟乩剞佶偈墼芨芰荠蒺蕺掎叽咭哜唧岌嵴洎屐骥畿玑楫殛戟戢赍觊犄齑矶羁嵇稷瘠虮笈笄暨跻跽霁鲚鲫髻'], ['Jia', '嘉枷夹佳家加荚颊贾甲钾假稼价架驾嫁伽郏葭岬浃迦珈戛胛恝铗镓痂瘕蛱笳袈跏'], ['Jian', '歼监坚尖笺间煎兼肩艰奸缄茧检柬碱硷拣捡简俭剪减荐槛鉴践贱见键箭件健舰剑饯渐溅涧建僭谏谫菅蒹搛湔蹇謇缣枧楗戋戬牮犍毽腱睑锏鹣裥笕翦踺鲣鞯'], ['Jiang', '僵姜将浆江疆蒋桨奖讲匠酱降茳洚绛缰犟礓耩糨豇'], ['Jiao', '蕉椒礁焦胶交郊浇骄娇搅铰矫侥脚狡角饺缴绞剿教酵轿较叫窖佼僬艽茭挢噍峤徼姣敫皎鹪蛟醮跤鲛'], ['Jue', '嚼撅攫抉掘倔爵觉决诀绝厥劂谲矍蕨噘崛獗孓珏桷橛爝镢蹶觖'], ['Jie', '揭接皆秸街阶截劫节桔杰捷睫竭洁结解姐戒芥界借介疥诫届讦诘拮喈嗟婕孑桀碣疖颉蚧羯鲒骱'], ['Jin', '巾筋斤金今津襟紧锦仅谨进靳晋禁近烬浸尽劲卺荩堇噤馑廑妗缙瑾槿赆觐衿矜'], ['Jing', '荆兢茎睛晶鲸京惊精粳经井警景颈静境敬镜径痉靖竟竞净刭儆阱菁獍憬泾迳弪婧肼胫腈旌箐'], ['Jiong', '炯窘迥扃'], ['Jiu', '揪究纠玖韭久灸九酒厩救旧臼舅咎就疚僦啾阄柩桕鸠鹫赳鬏'], ['Ju', '鞠拘狙疽居驹菊局咀矩举沮聚拒据巨具距踞锯俱句惧炬剧倨讵苣苴莒掬遽屦琚椐榘榉橘犋飓钜锔窭裾醵踽龃雎鞫'], ['Juan', '捐鹃娟倦眷卷绢鄄狷涓桊蠲锩镌隽'], ['Jun', '均菌钧军君峻俊竣浚郡骏捃皲筠麇'], ['Ka', '喀咖卡佧咔胩'], ['Luo', '咯萝螺罗逻锣箩骡裸落洛骆络倮蠃荦摞猡泺漯珞椤脶镙瘰雒'], ['Kai', '开揩楷凯慨剀垲蒈忾恺铠锎锴'], ['Kan', '刊堪勘坎砍看侃莰阚戡龛瞰'], ['Kang', '康慷糠扛抗亢炕伉闶钪'], ['Kao', '考拷烤靠尻栲犒铐'], ['Ke', '坷苛柯棵磕颗科壳咳可渴克刻客课嗑岢恪溘骒缂珂轲氪瞌钶锞稞疴窠颏蝌髁'], ['Ken', '肯啃垦恳裉'], ['Keng', '坑吭铿'], ['Kong', '空恐孔控倥崆箜'], ['Kou', '抠口扣寇芤蔻叩囗眍筘'], ['Ku', '枯哭窟苦酷库裤刳堀喾绔骷'], ['Kua', '夸垮挎跨胯侉'], ['Kuai', '块筷侩快蒯郐哙狯浍脍'], ['Kuan', '宽款髋'], ['Kuang', '匡筐狂框矿眶旷况诓诳邝圹夼哐纩贶'], ['Kui', '亏盔岿窥葵奎魁傀馈愧溃馗匮夔隗蒉揆喹喟悝愦逵暌睽聩蝰篑跬'], ['Kun', '坤昆捆困悃阃琨锟醌鲲髡'], ['Kuo', '括扩廓阔蛞'], ['La', '垃拉喇蜡腊辣啦剌邋旯砬瘌'], ['Lai', '莱来赖崃徕涞濑赉睐铼癞籁'], ['Lan', '蓝婪栏拦篮阑兰澜谰揽览懒缆烂滥岚漤榄斓罱镧褴'], ['Lang', '琅榔狼廊郎朗浪蒗啷阆稂螂'], ['Lao', '捞劳牢老佬姥酪烙涝唠崂忉栳铑铹痨耢醪'], ['Le', '勒了仂叻泐鳓'], ['Yue', '乐曰约越跃岳粤月悦阅龠哕瀹樾刖钺'], ['Lei', '雷镭蕾磊累儡垒擂肋类泪羸诔嘞嫘缧檑耒酹'], ['Leng', '棱楞冷塄愣'], ['Li', '厘梨犁黎篱狸离漓理李里鲤礼莉荔吏栗丽厉励砾历利傈例俐痢立粒沥隶力璃哩俪俚郦坜苈莅蓠藜呖唳喱猁溧澧逦娌嫠骊缡枥栎轹膦戾砺詈罹锂鹂疠疬蛎蜊蠡笠篥粝醴跞雳鲡鳢黧'], ['Lia', '俩'], ['Lian', '联莲连镰廉怜涟帘敛脸链恋炼练蔹奁潋濂琏楝殓臁裢裣蠊鲢'], ['Liang', '粮凉梁粱良两辆量晾亮谅墚莨椋锒踉靓魉'], ['Liao', '撩聊僚疗燎寥辽潦撂镣廖料蓼尥嘹獠寮缭钌鹩'], ['Lie', '列裂烈劣猎冽埒捩咧洌趔躐鬣'], ['Lin', '琳林磷霖临邻鳞淋凛赁吝拎蔺啉嶙廪懔遴檩辚瞵粼躏麟'], ['Ling', '玲菱零龄铃伶羚凌灵陵岭领另令酃苓呤囹泠绫柃棂瓴聆蛉翎鲮'], ['Liu', '溜琉榴硫馏留刘瘤流柳六浏遛骝绺旒熘锍镏鹨鎏'], ['Long', '龙聋咙笼窿隆垄拢陇垅茏珑栊胧砻癃'], ['Lou', '楼娄搂篓漏陋偻蒌喽嵝镂瘘耧蝼髅'], ['Lu', '芦卢颅庐炉掳卤虏鲁麓碌露路赂鹿潞禄录陆戮垆撸噜泸渌漉逯璐栌橹轳辂辘氇胪镥鸬鹭簏舻鲈'], ['Lv', '驴吕铝侣旅履屡缕虑氯律率滤绿捋闾榈膂稆褛'], ['Luan', '峦挛孪滦卵乱脔娈栾鸾銮'], ['Lue', '掠略锊'], ['Lun', '抡轮伦仑沦纶论囵'], ['Ma', '妈麻玛码蚂马骂嘛吗唛犸杩蟆'], ['Mai', '埋买麦卖迈脉劢荬霾'], ['Man', '瞒馒蛮满蔓曼慢漫谩墁幔缦熳镘颟螨鳗鞔'], ['Mang', '芒茫盲氓忙莽邙漭硭蟒'], ['Mao', '猫茅锚毛矛铆卯茂冒帽貌贸袤茆峁泖瑁昴牦耄旄懋瞀蝥蟊髦'], ['Me', '么麽'], ['Mei', '玫枚梅酶霉煤眉媒镁每美昧寐妹媚莓嵋猸浼湄楣镅鹛袂魅'], ['Mo', '没摸摹蘑模膜磨摩魔抹末莫墨默沫漠寞陌谟茉蓦馍嫫嬷殁镆秣瘼耱貊貘'], ['Men', '门闷们扪焖懑钔'], ['Meng', '萌蒙檬盟锰猛梦孟勐甍瞢懵朦礞虻蜢蠓艋艨'], ['Mi', '眯醚靡糜迷谜弥米秘觅泌蜜密幂芈谧咪嘧猕汨宓弭脒祢敉縻麋'], ['Mian', '棉眠绵冕免勉娩缅面沔渑湎腼眄'], ['Miao', '苗描瞄藐秒渺庙妙喵邈缈杪淼眇鹋'], ['Mie', '蔑灭乜咩蠛篾'], ['Min', '民抿皿敏悯闽苠岷闵泯缗玟珉愍黾鳘'], ['Ming', '明螟鸣铭名命冥茗溟暝瞑酩'], ['Miu', '谬缪'], ['Mou', '谋牟某侔哞眸蛑鍪'], ['Mu', '拇牡亩姆母墓暮幕募慕木目睦牧穆仫坶苜沐毪钼'], ['Na', '拿哪呐钠那娜纳讷捺肭镎衲'], ['Nai', '氖乃奶耐奈鼐佴艿萘柰'], ['Nan', '南男难喃囝囡楠腩蝻赧'], ['Nang', '囊攮囔馕曩'], ['Nao', '挠脑恼闹淖孬垴呶猱瑙硇铙蛲'], ['Ne', '呢'], ['Nei', '馁内'], ['Nen', '嫩恁'], ['Neng', '能'], ['Ni', '妮霓倪泥尼拟你匿腻逆溺伲坭蘼猊怩昵旎睨铌鲵'], ['Nian', '蔫拈年碾撵捻念廿埝辇黏鲇鲶'], ['Niang', '娘酿'], ['Niao', '鸟尿茑嬲脲袅'], ['Nie', '捏聂孽啮镊镍涅陧蘖嗫颞臬蹑'], ['Nin', '您'], ['Ning', '柠狞凝宁拧泞佞咛甯聍'], ['Niu', '牛扭钮纽拗狃忸妞'], ['Nong', '脓浓农弄侬哝'], ['Nu', '奴努怒弩胬孥驽'], ['Nv', '女恧钕衄'], ['Nuan', '暖'], ['Nue', '虐疟挪'], ['Nuo', '懦糯诺傩搦喏锘'], ['O', '哦噢'], ['Ou', '欧鸥殴藕呕偶沤讴怄瓯耦'], ['Pa', '啪趴爬帕怕琶葩杷筢'], ['Pai', '拍排牌徘湃派俳蒎哌'], ['Pan', '攀潘盘磐盼畔判叛拚爿泮袢襻蟠蹒'], ['Pang', '乓庞旁耪胖彷滂逄螃'], ['Pao', '抛咆刨炮袍跑泡匏狍庖脬疱'], ['Pei', '呸胚培裴赔陪配佩沛辔帔旆锫醅霈'], ['Pen', '喷盆湓'], ['Peng', '砰抨烹澎彭蓬棚硼篷膨朋鹏捧碰堋嘭怦蟛'], ['Pian', '篇偏片骗谝骈犏胼翩蹁'], ['Piao', '飘漂瓢票剽嘌嫖缥殍瞟螵'], ['Pie', '撇瞥丿苤氕'], ['Pin', '拼频贫品聘姘嫔榀牝颦'], ['Ping', '乒坪苹萍平凭瓶评屏俜娉枰鲆'], ['Po', '坡泼颇婆破魄迫粕叵鄱珀钋钷皤笸'], ['Pou', '剖裒掊'], ['Pu', '扑铺仆莆葡菩蒲埔朴圃普浦谱瀑匍噗溥濮璞氆镤镨蹼'], ['Qi', '期欺栖戚妻七凄漆柒沏其棋奇歧畦崎脐齐旗祈祁骑起岂乞企启契砌器气迄弃汽泣讫亓圻芑芪萁萋葺蕲嘁屺岐汔淇骐绮琪琦杞桤槭耆祺憩碛颀蛴蜞綦鳍麒'], ['Qia', '掐恰洽葜袷髂'], ['Qian', '牵扦钎铅千迁签仟谦乾黔钱钳前潜遣浅谴堑嵌欠歉倩佥阡芊芡茜掮岍悭慊骞搴褰缱椠肷愆钤虔箝'], ['Qiang', '枪呛腔羌墙蔷强抢戕嫱樯戗炝锖锵镪襁蜣羟跄'], ['Qiao', '橇锹敲悄桥瞧乔侨巧鞘撬翘峭俏窍劁诮谯荞愀憔缲樵硗跷鞒'], ['Qie', '切茄且怯窃郄惬妾挈锲箧趄'], ['Qin', '钦侵亲秦琴勤芹擒禽寝沁芩揿吣嗪噙溱檎锓螓衾'], ['Qing', '青轻氢倾卿清擎晴氰情顷请庆苘圊檠磬蜻罄綮謦鲭黥'], ['Qiong', '琼穷邛茕穹蛩筇跫銎'], ['Qiu', '秋丘邱球求囚酋泅俅巯犰湫逑遒楸赇虬蚯蝤裘糗鳅鼽'], ['Qu', '趋区蛆曲躯屈驱渠取娶龋趣去诎劬蕖蘧岖衢阒璩觑氍朐祛磲鸲癯蛐蠼麴瞿黢'], ['Quan', '圈颧权醛泉全痊拳犬券劝诠荃犭悛绻辁畎铨蜷筌鬈'], ['Que', '缺炔瘸却鹊榷确雀阕阙悫'], ['Qun', '裙群逡'], ['Ran', '然燃冉染苒蚺髯'], ['Rang', '瓤壤攘嚷让禳穰'], ['Rao', '饶扰绕荛娆桡'], ['Re', '惹热'], ['Ren', '壬仁人忍韧任认刃妊纫仞荏饪轫稔衽'], ['Reng', '扔仍'], ['Ri', '日'], ['Rong', '戎茸蓉荣融熔溶容绒冗嵘狨榕肜蝾'], ['Rou', '揉柔肉糅蹂鞣'], ['Ru', '茹蠕儒孺如辱乳汝入褥蓐薷嚅洳溽濡缛铷襦颥'], ['Ruan', '软阮朊'], ['Rui', '蕊瑞锐芮蕤枘睿蚋'], ['Run', '闰润'], ['Ruo', '若弱偌箬'], ['Sa', '撒洒萨卅脎飒'], ['Sai', '腮鳃塞赛噻'], ['San', '三叁伞散仨彡馓毵'], ['Sang', '桑嗓丧搡磉颡'], ['Sao', '搔骚扫嫂埽缫臊瘙鳋'], ['Se', '瑟色涩啬铯穑'], ['Sen', '森'], ['Seng', '僧'], ['Sha', '莎砂杀刹沙纱傻啥煞唼歃铩痧裟霎鲨'], ['Shai', '筛晒酾'], ['Shan', '珊苫杉山删煽衫闪陕擅赡膳善汕扇缮讪鄯芟潸姗嬗骟膻钐疝蟮舢跚鳝'], ['Shang', '墒伤商赏晌上尚裳垧泷绱殇熵觞'], ['Shao', '梢捎稍烧芍勺韶少哨邵绍劭潲杓筲艄'], ['She', '奢赊蛇舌舍赦摄射慑涉社设厍佘揲猞滠麝'], ['Shen', '砷申呻伸身深娠绅神沈审婶甚肾慎渗什诜谂莘葚哂渖椹胂矧蜃糁'], ['Sheng', '声生甥牲升绳省盛剩胜圣嵊晟眚笙'], ['Shou', '收手首守寿授售受瘦兽狩绶艏'], ['Shu', '蔬枢梳殊抒输叔舒淑疏书赎孰熟薯暑曙署蜀黍鼠属术述树束戍竖墅庶数漱恕丨倏塾菽摅沭澍姝纾毹腧殳秫'], ['Shua', '刷耍唰'], ['Shuai', '摔衰甩帅蟀'], ['Shuan', '栓拴闩涮'], ['Shuang', '霜双爽孀'], ['Shui', '谁水睡税'], ['Shun', '吮瞬顺舜'], ['Shuo', '说硕朔烁蒴搠妁槊铄'], ['Si', '斯撕嘶思私司丝死肆寺嗣四伺似饲巳厮俟兕厶咝汜泗澌姒驷缌祀锶鸶耜蛳笥'], ['Song', '松耸怂颂送宋讼诵凇菘崧嵩忪悚淞竦'], ['Sou', '搜艘擞嗽叟薮嗖嗾馊溲飕瞍锼螋'], ['Su', '苏酥俗素速粟僳塑溯宿诉肃夙谡蔌嗉愫涑簌觫稣'], ['Suan', '酸蒜算狻'], ['Sui', '虽隋随绥髓碎岁穗遂隧祟谇荽濉邃燧眭睢'], ['Sun', '孙损笋荪狲飧榫隼'], ['Suo', '蓑梭唆缩琐索锁所唢嗦嗍娑桫挲睃羧'], ['Ta', '塌他它她塔獭挞蹋踏嗒闼溻遢榻沓铊趿鳎'], ['Tai', '胎苔抬台泰酞太态汰邰薹骀肽炱钛跆鲐'], ['Tang', '汤塘搪堂棠膛唐糖倘躺淌趟烫傥帑溏瑭樘铴镗耥螗螳羰醣'], ['Tao', '掏涛滔绦萄桃逃淘陶讨套鼗啕洮韬饕'], ['Te', '特忑慝铽'], ['Teng', '藤腾疼誊滕'], ['Ti', '梯剔踢锑提题蹄啼体替嚏惕涕剃屉倜悌逖绨缇鹈裼醍'], ['Tian', '天添填田甜恬舔腆掭忝阗殄畋'], ['Tie', '贴铁帖萜餮'], ['Ting', '厅听烃汀廷停亭庭挺艇莛葶婷梃铤蜓霆'], ['Tong', '通桐酮瞳同铜彤童桶捅筒统痛佟仝茼嗵恸潼砼'], ['Tou', '偷投头透骰'], ['Tu', '凸秃突图徒途涂屠土吐兔堍荼菟钍酴'], ['Tuan', '湍团抟彖疃'], ['Tui', '推颓腿蜕褪退忒煺'], ['Tuo', '拖托脱鸵陀驮驼椭妥拓唾佗坨庹沱柝柁橐砣箨酡跎鼍'], ['Wa', '挖哇蛙洼娃瓦袜佤娲腽'], ['Wai', '歪外崴'], ['Wan', '豌弯湾玩顽丸烷完碗挽晚皖惋宛婉万腕剜芄菀纨绾琬脘畹蜿'], ['Wang', '汪王亡枉网往旺望忘妄罔惘辋魍'], ['Wei', '威巍微危韦违桅围唯惟为潍维苇萎委伟伪尾纬未蔚味畏胃喂魏位渭谓尉慰卫偎诿隈葳薇帏帷嵬猥猬闱沩洧涠逶娓玮韪軎炜煨痿艉鲔'], ['Wen', '瘟温蚊文闻纹吻稳紊问刎阌汶璺攵雯'], ['Weng', '嗡翁瓮蓊蕹'], ['Wo', '挝蜗涡窝我斡卧握沃倭莴喔幄渥肟硪龌'], ['Wu', '巫呜钨乌污诬屋无芜梧吾吴毋武五捂午舞伍侮坞戊雾晤物勿务悟误兀仵阢邬圬芴唔庑怃忤寤迕妩婺骛杌牾焐鹉鹜痦蜈鋈鼯'], ['Xi', '昔熙析西硒矽晰嘻吸锡牺稀息希悉膝夕惜熄烯溪汐犀檄袭席习媳喜铣洗系隙戏细僖兮隰郗菥葸蓰奚唏徙饩阋浠淅屣嬉玺樨曦觋欷歙熹禊禧皙穸蜥螅蟋舄舾羲粞翕醯蹊鼷'], ['Xia', '瞎虾匣霞辖暇峡侠狭下厦夏吓呷狎遐瑕柙硖罅黠'], ['Xian', '掀锨先仙鲜纤咸贤衔舷闲涎弦嫌显险现献县腺馅羡宪陷限线冼苋莶藓岘猃暹娴氙燹祆鹇痫蚬筅籼酰跣跹霰'], ['Xiang', '相厢镶香箱襄湘乡翔祥详想响享项巷橡像向象芗葙饷庠骧缃蟓鲞飨'], ['Xiao', '萧硝霄削哮嚣销消宵淆晓小孝校肖啸笑效哓崤潇逍骁绡枭枵蛸筱箫魈'], ['Xie', '楔些歇蝎鞋协挟携邪斜胁谐写械卸蟹懈泄泻谢屑偕亵勰燮薤撷獬廨渫瀣邂绁缬榭榍蹀躞'], ['Xin', '薪芯锌欣辛新忻心信衅囟馨昕歆镡鑫'], ['Xing', '星腥猩惺兴刑型形邢醒幸杏性姓陉荇荥擤饧悻硎'], ['Xiong', '兄凶胸匈汹雄熊芎'], ['Xiu', '休修羞朽嗅锈秀袖绣咻岫馐庥溴鸺貅髹'], ['Xu', '墟戌需虚嘘须徐许蓄酗叙旭序畜恤絮婿绪续诩勖圩蓿洫溆顼栩煦盱胥糈醑'], ['Xuan', '轩喧宣悬旋玄选癣眩绚儇谖萱揎泫渲漩璇楦暄炫煊碹铉镟痃'], ['Xue', '靴薛学穴雪血谑噱泶踅鳕'], ['Xun', '勋熏循旬询寻驯巡殉汛训讯逊迅巽郇埙荀荨蕈薰峋徇獯恂洵浔曛醺鲟'], ['Ya', '压押鸦鸭呀丫芽牙蚜崖衙涯雅哑亚讶伢垭揠岈迓娅琊桠氩砑睚痖'], ['Yan', '焉咽阉烟淹盐严研蜒岩延言颜阎炎沿奄掩眼衍演艳堰燕厌砚雁唁彦焰宴谚验厣赝剡俨偃兖谳郾鄢埏菸崦恹闫阏湮滟妍嫣琰檐晏胭焱罨筵酽趼魇餍鼹'], ['Yang', '殃央鸯秧杨扬佯疡羊洋阳氧仰痒养样漾徉怏泱炀烊恙蛘鞅'], ['Yao', '邀腰妖瑶摇尧遥窑谣姚咬舀药要耀钥夭爻吆崾徭幺珧杳轺曜肴铫鹞窈鳐'], ['Ye', '椰噎耶爷野冶也页掖业叶曳腋夜液靥谒邺揶晔烨铘'], ['Yi', '一壹医揖铱依伊衣颐夷遗移仪胰疑沂宜姨彝椅蚁倚已乙矣以艺抑易邑屹亿役臆逸肄疫亦裔意毅忆义益溢诣议谊译异翼翌绎刈劓佚佾诒圯埸懿苡荑薏弈奕挹弋呓咦咿噫峄嶷猗饴怿怡悒漪迤驿缢殪轶贻欹旖熠眙钇镒镱痍瘗癔翊蜴舣羿'], ['Yin', '茵荫因殷音阴姻吟银淫寅饮尹引隐印胤鄞垠堙茚吲喑狺夤洇氤铟瘾窨蚓霪龈'], ['Ying', '英樱婴鹰应缨莹萤营荧蝇迎赢盈影颖硬映嬴郢茔莺萦蓥撄嘤膺滢潆瀛瑛璎楹媵鹦瘿颍罂'], ['Yo', '哟唷'], ['Yong', '拥佣臃痈庸雍踊蛹咏泳涌永恿勇用俑壅墉喁慵邕镛甬鳙饔'], ['You', '幽优悠忧尤由邮铀犹油游酉有友右佑釉诱又幼卣攸侑莠莜莸尢呦囿宥柚猷牖铕疣蚰蚴蝣繇鱿黝鼬'], ['Yu', '迂淤于盂榆虞愚舆余俞逾鱼愉渝渔隅予娱雨与屿禹宇语羽玉域芋郁吁遇喻峪御愈欲狱育誉浴寓裕预豫驭禺毓伛俣谀谕萸蓣揄圄圉嵛狳饫馀庾阈鬻妪妤纡瑜昱觎腴欤於煜燠聿畲钰鹆鹬瘐瘀窬窳蜮蝓竽臾舁雩龉'], ['Yuan', '鸳渊冤元垣袁原援辕园员圆猿源缘远苑愿怨院垸塬芫掾沅媛瑗橼爰眢鸢螈箢鼋'], ['Yun', '耘云郧匀陨允运蕴酝晕韵孕郓芸狁恽愠纭韫殒昀氲熨'], ['Za', '匝砸杂咋拶咂'], ['Zai', '栽哉灾宰载再在崽甾'], ['Zan', '咱攒暂赞瓒昝簪糌趱錾'], ['Zang', '赃脏葬奘驵臧'], ['Zao', '遭糟凿藻枣早澡蚤躁噪造皂灶燥唣'], ['Ze', '责择则泽仄赜啧帻迮昃箦舴'], ['Zei', '贼'], ['Zen', '怎谮'], ['Zeng', '增憎赠缯甑罾锃'], ['Zha', '扎喳渣札轧铡闸眨栅榨乍炸诈柞揸吒咤哳楂砟痄蚱齄'], ['Zhan', '瞻毡詹粘沾盏斩辗崭展蘸栈占战站湛绽谵搌旃'], ['Zhang', '樟章彰漳张掌涨杖丈帐账仗胀瘴障仉鄣幛嶂獐嫜璋蟑'], ['Zhao', '招昭找沼赵照罩兆肇召着诏棹钊笊'], ['Zhe', '遮折哲蛰辙者锗蔗这浙乇谪摺柘辄磔鹧褶蜇螫赭'], ['Zhen', '珍斟真甄砧臻贞针侦枕疹诊震振镇阵帧圳蓁浈缜桢榛轸赈胗朕祯畛稹鸩箴'], ['Zheng', '蒸挣睁征狰争怔整拯正政症郑证诤峥徵钲铮筝'], ['Zhi', '芝枝支吱蜘知肢脂汁之织职直植殖执值侄址指止趾只旨纸志挚掷至致置帜峙制智秩稚质炙痔滞治窒卮陟郅埴芷摭帙忮彘咫骘栉枳栀桎轵轾贽胝膣祉黹雉鸷痣蛭絷酯跖踬踯豸觯'], ['Zhong', '中盅忠钟衷终种肿仲众冢锺螽舯踵'], ['Zhou', '舟周州洲诌粥轴肘帚咒皱宙昼骤荮啁妯纣绉胄碡籀酎'], ['Zhu', '珠株蛛朱猪诸诛逐竹烛煮拄瞩嘱主著柱助蛀贮铸筑住注祝驻伫侏邾苎茱洙渚潴杼槠橥炷铢疰瘃竺箸舳翥躅麈'], ['Zhua', '抓爪'], ['Zhuai', '拽'], ['Zhuan', '专砖转撰赚篆啭馔颛'], ['Zhui', '椎锥追赘坠缀萑惴骓缒隹'], ['Zhun', '谆准肫窀'], ['Zhuo', '捉拙卓桌琢茁酌啄灼浊倬诼擢浞涿濯禚斫镯'], ['Zi', '兹咨资姿滋淄孜紫仔籽滓子自渍字谘呲嵫姊孳缁梓辎赀恣眦锱秭耔笫粢趑訾龇鲻髭'], ['Zong', '鬃棕踪宗综总纵偬腙粽'], ['Zou', '邹走奏揍诹陬鄹驺鲰'], ['Zu', '租足族祖诅阻组俎菹镞'], ['Zuan', '钻纂攥缵躜'], ['Zui', '嘴醉最罪蕞觜'], ['Zun', '尊遵撙樽鳟'], ['Zuo', '昨左佐做作坐座阼唑怍胙祚笮'], ['Ei', '诶'], ['Dia', '嗲'], ['Cen', '岑涔'], ['Nou', '耨']],
		getPinyin: function(word) {
			var oResult = Dota.get(word);
			return oResult.p;
		},
		getFirstLetter: function(word) {
			var oResult = Dota.get(word);
			return oResult.l;
		},
		get: function(words) {
			words = words || "";
			var sWord, aP = [],
			aL = [];
			for (var i = 0, end = words.length; i < end; i++) {
				sWord = words.charAt(i);
				oResult = Dota.getWord(sWord);
				aP.push(oResult.p);
				aL.push(oResult.l);
			}
			return {
				p: aP.join(''),
				l: aL.join('')
			}
		},
		getWord: function(word) {
			var oResult = {
				p: "",
				l: ""
			};
			if (!word) {
				return oResult;
			}
			var db = Dota.db;
			var aDbGroup, sDbGroupWords, i, iEnd, n, nEnd, sPinyin;
			for (i = 0, iEnd = db.length; i < iEnd; i++) {
				sPinyin = word;
				aDbGroup = db[i];
				sDbGroupWords = aDbGroup[1];
				if (sDbGroupWords.indexOf(word) !== - 1) {
					sPinyin = aDbGroup[0];
					break;
				}
			}
			var sLower = sPinyin.toLowerCase();
			oResult = {
				p: sLower,
				l: sLower.charAt(0)
			};
			return oResult;
		}
	};
	window.Pinyin = Dota;
})();
$.tablesorter.addParser({
	id: "rmb",
	is: function(s) {
		return false;
	},
	format: function(s) {
		return parseFloat(s.toLowerCase().replace("￥", "").replace("-", "0"));
	},
	type: "numeric",
});
$.tablesorter.addParser({
	id: "pinyin",
	is: function(s) {
		return false;
	},
	format: function(s) {
		return Pinyin.getPinyin(s);
	},
	type: "text",
});

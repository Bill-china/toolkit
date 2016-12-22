var month_tip_str = "亲，你太黑了，咱最多能看6个月的数据，行么？";
var start = new Month({
	year: "start_year",
	month: "start_month",
	end: 2014
}).change(function() {
	if ((parseInt(end.getYear()) * 12 + parseInt(end.getMonth())) - (parseInt(this.getYear()) * 12 + parseInt(this.getMonth())) > 6) {
		alert(month_tip_str);
	}
});
var end = new Month({
	year: "end_year",
	month: "end_month",
	end: 2014
}).change(function() {
	if ((parseInt(this.getYear()) * 12 + parseInt(this.getMonth())) - (parseInt(start.getYear()) * 12 + parseInt(start.getMonth())) > 6) {
		alert(month_tip_str);
	}
});


(function($){
	$.fn.dateshow = function(options){
		var defaults = {
			minDate: '1970',
			maxDate: '2030',
			onfocus:function(text){
				alert(text);
			},
		}
		
		var options = $.extend(defaults, options);
		var s=options.minDate,e=options.maxDate;
		for(var i=s;i<=e;i++){
			$(this).append('<option value="'+i+'">'+i+'</option>');
		}
		$(this).change(function(){
			options.onfocus(this.value);
		})
    };
})(jQuery);
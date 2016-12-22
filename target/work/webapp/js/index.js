/**
 * load url to container
 * 
 * @param url
 */
function changeTo(url) {
	$('#content').empty();
	$('#content').load(url);
}

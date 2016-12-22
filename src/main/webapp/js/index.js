
/**
 * @author xujie-iri
 * @time 2016-7-15
 */

/**
 * load url to container
 * @param url
 */
function changeTo(url) {
	$('#content').empty();
	$('#content').load(url);
}

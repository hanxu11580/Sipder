function decode(URL) {
	var inflateData = pako.inflate(URL, {
		to : 'string'
	});
	return inflateData;
}
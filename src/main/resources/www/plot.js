$(document).ready(function() {

  // Initialize variables
  var kTrump = 10000,
    kBitcoin = 1000,
    bitcoinData = {},
    trumpData = {};

  // Initial setup
  updateTrump();
  updateBitcoin();

  // Trump input handling
  $('#sliderTrump').change(function(){
    kTrump = Math.pow(10, this.value);
    $('#kTrump').html(d3.format(",d")(kTrump));
    updateTrump();
  });

  // Bitcoin input handling
  $('#sliderBitcoin').change(function(){
    kBitcoin = Math.pow(10, this.value);
    $('#kBitcoin').html(d3.format(",d")(kBitcoin));
    updateBitcoin();
  });

  // Update logic
  setInterval(updateTrump, 500);
  setInterval(updateBitcoin, 500);

  // Redraw on resize
  window.addEventListener("resize", function() {
    drawTrump();
    drawBitcoin();
  });

  // Trump update wrapper
  function updateTrump(){
    $.get("counts/trump?k=" + kTrump, function(data) {
      trumpData = data;
      drawTrump();
    });
  }

  // Bitcoin update wrapper
  function updateBitcoin(){
    $.get("counts/bitcoin?k=" + kBitcoin, function(data) {
      bitcoinData = data;
      drawBitcoin();
    });
  }

  // Trump plotting
  function drawTrump() {

    // Define parameters
    var transitionMs = 250,
      chartDiv = document.getElementById("trump"),
      counts = trumpData.labelCounts;
    var barHeight = 25,
      margin = {top: 50, right: 10, bottom: 20, left: 5},
      chartWidth = +chartDiv.clientWidth,
      chartHeight = barHeight*counts.length + margin.top + margin.bottom;

    // Update title and size
    writeGraphTitle(trumpData.positionsInWindow, chartDiv);
    d3.select(chartDiv).select("svg")
        .attr("width", chartWidth)
        .attr("height", chartHeight);

    // Setup axis scaling
    var x = d3.scaleLinear()
        .domain([0, d3.max(counts, function(d) { return d.count; })])
        .range([0, chartWidth - margin.left - margin.right]);

    // Draw axis
    d3.select(chartDiv).select(".axis")
        .attr("transform", "translate(" + margin.left + "," + (margin.top - 5) + ")")
        .call(d3.axisTop(x).ticks(5));
    d3.select(chartDiv).select(".axis-label")
      .selectAll("text")
        .attr("x", +chartDiv.clientWidth)
        .attr("y", margin.top)
        .attr("dy", ".71em")
        .attr("fill", "#000")
        .attr("text-anchor", "end")
        .attr("font-size", "16px");

    // Join bars to data
    counts.sort(compareHashtags);
    var bars = d3.select(chartDiv).select(".bars")
        .attr("transform", "translate(" + margin.left + ", " + margin.top + ")");
    var updated = bars.selectAll("g")
      .data(counts, function(d){ return d.label; });

    // Initialize new bars
    var entered = updated.enter().append("g")
        .attr("transform", function(d, i){ return "translate(0," + i*barHeight + ")"; });
    entered.append("rect")
        .attr("height", barHeight - 1)
        .attr("width", function(d){ return x(d.count); })
        .on("mouseover", function(d) { updateTooltip(d, this, display=true); })
        .on("mouseout", function(d) { updateTooltip(d, this, display=false); });
    entered.append("a")
        .attr("href", function(d){
          return "https://twitter.com/search?q=" + encodeURIComponent(d.label);
        })
        .attr("target", "_blank")
      .append("text")
        .attr("x", 0)
        .attr("y", barHeight/2)
        .attr("dy", ".35em")
        .attr("text-anchor", "start")
        .attr("text-decoration", "underline")
        .attr("font-size", "13px")
        .text(function(d) { return d.label; })
        .on("mouseout", function() { d3.select(this).style("fill", "black"); })
        .on("mouseover", function() {
          d3.select(this).style("fill", "blue")
            .style("cursor", "pointer");
        });

    // Remove old bars
    updated.exit().transition().duration(transitionMs).remove();

    // Update existing bars
    updated.transition().duration(transitionMs)
        .attr("transform", function(d, i){ return "translate(0," + i*barHeight + ")"; });
    bars.selectAll("g").sort(compareHashtags);
    bars.selectAll("rect").data(counts, function(d, i){ return i; })
      .transition().duration(transitionMs)
        .attr("width", function(d){ return x(d.count); });
  }

  // Bitcoin plotting
  function drawBitcoin() {

    // Define parameters
    var transitionMs = 250,
      chartDiv = document.getElementById("bitcoin"),
      counts = bitcoinData.labelCounts;
    var margin = {top: 50, right: 10, bottom: 100, left: 65},
      chartHeight = 700,
      barMaxHeight = chartHeight - margin.top - margin.bottom,
      barWidth = (+chartDiv.clientWidth - margin.left - margin.right)/counts.length;

    // Update title and size
    writeGraphTitle(bitcoinData.positionsInWindow, chartDiv);
    d3.select(chartDiv).select("svg")
          .attr("width", +chartDiv.clientWidth)
          .attr("height", chartHeight);

    // Setup axis scaling
    counts.sort(compareBitcoin);
    var x = d3.scaleBand()
          .domain(counts.map(function(d) { return d.label; }))
          .range([0, barWidth*counts.length]),
      y = d3.scaleLinear()
          .domain([0, d3.max(counts, function(d) { return d.count; })])
          .range([barMaxHeight, 0]);

    // Draw axes
    d3.select(chartDiv).select(".xaxis")
        .attr("transform", "translate(" + margin.left + "," + (margin.top + barMaxHeight) + ")")
        .call(d3.axisBottom(x).ticks())
      .selectAll("text")
        .attr("x", "-10")
        .attr("y", "7")
        .attr("dy", ".35em")
        .attr("transform", "rotate(-45)")
        .attr("text-anchor", "end");
    d3.select(chartDiv).select(".yaxis")
        .attr("transform", "translate(" + (margin.left - 5) + "," + margin.top + ")")
        .call(d3.axisLeft(y).ticks());
    d3.select(chartDiv).select(".xaxis-label").selectAll("text")
        .attr("x", +chartDiv.clientWidth)
        .attr("y", barMaxHeight + margin.top - 15)
        .attr("dy", ".71em")
        .attr("fill", "#000")
        .attr("text-anchor", "end")
        .attr("font-size", "16px");
    d3.select(chartDiv).select(".yaxis-label").selectAll("text")
 	    .attr("x", -1*margin.top)
 	    .attr("y", margin.left)
        .attr("dy", ".71em")
        .attr("transform", "rotate(-90)")
        .attr("text-anchor", "end")
        .attr("fill", "#000")
        .attr("font-size", "16px");

    // Join bars to data
    var bars = d3.select(chartDiv).select(".bars")
        .attr("transform", "translate(" + margin.left + "," + margin.top + ")");
    var updated = bars.selectAll("g")
      .data(counts, function(d){ return d.label; });

    // Initialize new bars
    var entered = updated.enter().append("g")
        .attr("transform", function(d){ return "translate(" + x(d.label) + ",0)"; });
    entered.append("rect")
        .attr("x", 0)
        .attr("y", function(d) { return y(d.count);} )
        .attr("height", function(d) { return barMaxHeight - y(d.count); })
        .attr("width", 0.9*barWidth)
        .on("mouseover", function(d) { updateTooltip(d, this, display=true); })
        .on("mouseout", function(d) { updateTooltip(d, this, display=false); });

    // Remove old bars
    updated.exit().transition().duration(transitionMs).remove();

    // Update existing bars
    updated.transition().duration(transitionMs)
        .attr("transform", function(d, i){ return "translate(" + x(d.label) + ",0)"; });
    bars.selectAll("g").sort(compareBitcoin);
    bars.selectAll("rect").data(counts, function(d, i){ return i; })
      .transition().duration(transitionMs)
        .attr("y", function(d) { return y(d.count); })
        .attr("height", function(d) { return barMaxHeight - y(d.count); })
        .attr("width", 0.9*barWidth);
  }

  // Updates text above graphs
  function writeGraphTitle(positionsInWindow, chartDiv) {
    d3.select(chartDiv).select(".window-size")
        .text(d3.format(",d")(positionsInWindow));
  }

  // Handles tooltip logic
  function updateTooltip(d, element, display=true) {
    var tooltip = d3.select(".tooltip");
    if (display) {
      d3.select(element).style("fill-opacity", "0.7");
      tooltip
          .html("<b>" + d.label + "</b> <br> Approximate Count: " + d.count)
          .style("left", (d3.event.pageX) + "px")
          .style("top", (d3.event.pageY - 28) + "px");
      tooltip.transition().duration(50).style("opacity", .9);
    } else {
      d3.select(element).style("fill-opacity", "0.3");
      tooltip.transition().duration(50).style("opacity", 0);
    }
  }

  // Ordering function for hashtags
  function compareHashtags(d1, d2) {
    if (d2.count < d1.count) return -1;
    if (d2.count > d1.count) return 1;
    if (d2.label < d1.label) return -1;
    if (d2.label > d1.label) return 1;
    return 0;
  }

  // Ordering function for bitcoin labels
  function compareBitcoin(d1, d2) {
    var v1 = parseFloat(d1.label.split(" ")[0]);
    var v2 = parseFloat(d2.label.split(" ")[0]);;
    if (v2 < v1) return 1;
    if (v2 > v1) return -1;
    return 0;
  }
});
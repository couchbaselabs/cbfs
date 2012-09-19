var repCountURL = '/.cbfs/viewproxy/cbfs/_design/cbfs/_view/repcounts?group_level=1';
var refreshInterval = 2000;

function prettySize(s) {
    if (s < 10) {
        return s + "B";
	}
	var e = parseInt(Math.floor(Math.log(s) / Math.log(1024)));
    var sizes = ["B", "KB", "MB", "GB", "TB", "PB", "EB"];
	var suffix = sizes[parseInt(e)];
	var val = s / Math.pow(1024, Math.floor(e));
    return val.toFixed(2) + suffix;
}

function updateBubbles(bubble, vis, d) {
    if (!d) {
        return;
    }
    var format = d3.format(",d");

    var children = [];
    for (var n in d) {
        children.push({age: d[n].hbage_ms,
                       hbs: d[n].hbage_str,
                       node: n,
                       value: d[n].size});
    }

    function fill(val) {
        var offset = Math.min(1, val / 180000);
        return d3.interpolateRgb("#bbf", "#f66")(offset);
    }

    var data = bubble.nodes({children: children})
        .filter(function(d) { return !d.children; });

    var node = vis.selectAll("g.node")
        .data(data)
      .enter().append("g")
        .attr("class", "node")
        .attr("transform", function(d) { return "translate(" + d.x + "," + d.y + ")"; });

    vis.selectAll("g.node")
        .data(data)
      .transition()
        .duration(1000)
        .attr("transform", function(d) { return "translate(" + d.x + "," + d.y + ")"; });

    vis.selectAll("g.node")
        .data(data)
      .exit().remove();

    node.append("title")
        .text(function(d) { return d.node + ": " + format(d.value); });

    node.append("circle")
        .attr("r", function(d) { return d.r; })
        .style("fill", function(d) { return fill(d.age); });

    node.append("text")
        .attr("text-anchor", "middle")
        .attr("dy", ".3em")
        .text(function(d) { return d.node.substring(0, d.r / 3); });

    vis.selectAll("g.node text")
        .data(data)
        .text(function(d) {
            return d.node + " " + prettySize(d.value);
        });

    vis.selectAll("g.node circle")
        .data(data)
      .transition()
        .duration(1000)
        .style("fill", function(d) {
            return fill(d.age);
        })
        .attr("r", function(d) { return d.r; });

    vis.selectAll("g.node title")
        .data(data)
        .text(function(d) {
            return "Last heartbeat from " + d.node + " " + d.hbs + " ago";
        });

}

function drawBubbles(d) {
    var r = Math.min(window.innerWidth, window.innerHeight);
    var bubble = d3.layout.pack()
        .sort(null)
        .size([r, r])
        .padding(1.5);

    var vis = d3.select("#bubbles").append("svg")
        .attr("width", r)
        .attr("height", r)
        .attr("class", "bubble");

    updateBubbles(bubble, vis, d);
    setInterval(function() {
        d3.json("/.cbfs/nodes/", function(d) {
            updateBubbles(bubble, vis, d);
        });
    }, refreshInterval);
}

function drawRepcounts(d) {
    console.log(d);

    var vals = [],
        names = [];

    for (var i = 0; i < d.rows.length; i++) {
        names.push(d.rows[i].key);
        vals.push(parseInt(d.rows[i].value));
    }

    var w = 200, bh = 20;

    var x = d3.scale.linear()
        .domain([0, d3.max(vals)])
        .range([0, w]);

    var textify = function(d, i) {
        return names[i] + " rep: " + d;
    };

    var repChart = d3.select("#repcounts svg");

    repChart.selectAll("rect")
        .data(vals)
      .enter().append("rect")
        .attr("y", function(d, i) { return i * bh; })
        .attr("width", x)
        .attr("x", 0)
        .attr("height", bh);

    repChart.selectAll("rect")
        .data(vals)
      .exit().remove();

    repChart.selectAll("rect")
        .data(vals)
        .attr("class", function(d, i) {
            return parseInt(names[i]) < 2 ? "under" : null;
        })
      .transition()
        .attr("width", x)
        .attr("x", 0);

    repChart.selectAll("text")
        .data(vals)
      .enter().append("text");

    repChart.selectAll("text")
        .data(vals)
      .exit().remove();

    repChart.selectAll("text")
        .data(vals)
        .attr("x", 10)
        .attr("y", function(d, i) { return bh * (1 + i);})
        .attr("dx", -3)
        .attr("dy", "-5")
        .attr("text-anchor", "start")
        .text(textify);
}

function monitorInit() {
    console.log("Starting monitoring");

    var repChart = d3.select("#repcounts").append("svg")
        .attr("class", "chart")
        .attr("width", 200);

    d3.json("/.cbfs/nodes/", drawBubbles);
    setInterval(function() {
        d3.json(repCountURL, drawRepcounts);
    }, refreshInterval);
}

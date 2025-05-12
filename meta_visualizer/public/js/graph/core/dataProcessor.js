// Data Processor - Processes metadata into nodes and links
/**
 * Process metadata from a database schema/model into a graph representation
 * with nodes (tables/collections) and links (relationships).
 *
 * This is the main entry point for converting database metadata into a
 * visualization-ready format.
 *
 * @param {Object} data - The metadata from the JSON file containing database schema information
 * @returns {Object} An object containing nodes, links arrays and the graph name:
 *   - nodes: Array of collection nodes with their properties and visual attributes
 *   - links: Array of relationships between collections
 *   - graphName: The name of the processed graph
 */
export function processMetadata(data) {
  const nodes = [];
  const links = [];

  // Extract the first graph from the metadata
  const graphName = Object.keys(data)[0];
  if (!graphName) {
    console.warn("No graph data found");
    return { nodes, links, graphName: null };
  }

  // TODO: support multiple graphs
  const graph = data[0];

  const collections = graph.collections || [];
  const relationships = graph.relationships || [];

  console.log("Collections found:", Object.keys(collections).length);
  console.log("Relationships found:", Object.keys(relationships).length);

  // Process all collections to create nodes
  processCollections(collections, nodes);

  //  Create links based on all subcollection relationships
  processRelationships(relationships, nodes, links);

  // Post-process the nodes after the links are created
  postProcessCollections(nodes);
  
  console.log("Nodes: ", nodes);
  console.log("Links: ", links);

  return { nodes, links, graphName };
}

/**
 * Process database collections into graph nodes with visual properties.
 *
 * Iterates through each collection in the metadata, extracts its properties (columns)
 * and relationships (subcollections), and creates a node object with positioning
 * information for graph visualization.
 *
 * @param {Object} collections - The collections from the metadata, where keys are collection names
 * @param {Array} nodes - The array to store created node objects (modified in-place)
 */
function processCollections(collections, nodes) {
  collections.forEach(
    (collectionData, index) => {
      const collectionName = collectionData.name || "Unknown Collection";
      console.log(`Processing collection: ${collectionName}`);

      // Extract table columns for display
      const columns = [];
      processProperties(collectionData, columns);

      // Create the node object with visual coordinates and data properties
      nodes.push({
        id: collectionName,
        name: collectionName,
        columns: columns,
        sub_collections: [],
        tablePath: collectionData["table path"] || "No path specified",
        uniqueProperties: collectionData["unique properties"] || ["No unique properties specified"],

        // Position nodes in a grid pattern for initial visualization
        x: 150 + (index % 3) * 500,
        y: 150 + Math.floor(index / 3) * 400,
        width: 350,

        // These properties are used for hierarchical tree visualization layouts
        treePosition: null,
        treeLevel: null,
        treeParent: null,
      });
    }
  );

  console.log(`Created ${nodes.length} nodes`);
}


/**
 * Processes each regular scalar property in a collection
 *
 * @param {Object} collectionData - The collection data containing properties and metadata
 * @param {Array} columns - Output array to store column information (modified in-place)
 */
function processProperties(
  collectionData,
  columns,
) {
  collectionData.properties.forEach(propData => {
    // Handle regular table columns (attributes)
    if (propData.type === "table column") {
      columns.push({
        name: propData.name,
        columnName: propData["column name"],
        dataType: propData["data type"],
      });
    }
  });
}

/**
 * TODO
 *
 * @param {Array} nodes - The array containing the node objects (modified in-place)
 */
function postProcessCollections(nodes) {
  nodes.forEach(
    (node, _) => {
      console.log(`Post-processing node: ${node.id}`);
      // Height is dynamic based on number of properties to ensure all are visible
      node.height = Math.max(
        250,
        150 + (node.columns.length + node.sub_collections.length) * 22
      );
    }
  );

  console.log(`Finished post-processing nodes`);
}

/**
 * TODO
 *
 * @param {Array} nodes - The array of all relationship objects
 * @param {Array} nodes - The array of all node objects
 * @param {Array} links - The array to store the created link objects (modified in-place)
 */
function processRelationships(relationships, nodes, links) {
  relationships.forEach((relationshipData) => {
    const relationshipName = relationshipData.name || "Unknown Relationship";
    console.log(`Processing relationship: ${relationshipName}`);
    const relationshipType = relationshipData.type || "unknown";
    if (["simple join", "general join", "cartesian product"].includes(relationshipType)) {
      const sourceNode = nodes.find((n) => n.id === relationshipData["parent collection"]);
      const targetNode = nodes.find((n) => n.id === relationshipData["child collection"]);
      const link = {
        source: sourceNode.id,
        target: targetNode.id,
        type: relationshipType,
        name: relationshipName,
        data: relationshipData,
      };
      sourceNode.sub_collections.push(link);
      links.push(link);
    } else if (relationshipType === "reverse") {
      const targetNode = nodes.find((n) => n.id === relationshipData["original parent"]);
      const originalProperty = targetNode.sub_collections.find((l) => l.name === relationshipData["original property"]);
      const sourceNode = nodes.find((n) => n.id === originalProperty.target);
      const link = {
        source: sourceNode.id,
        target: targetNode.id,
        type: "reverse",
        name: relationshipName,
        data: relationshipData,
      }
      sourceNode.sub_collections.push(link);
      links.push(link);
    }
  });
}

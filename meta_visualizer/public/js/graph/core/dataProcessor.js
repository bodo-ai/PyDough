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

  const collections = data[graphName] || {};
  console.log("Collections found:", Object.keys(collections).length);

  // First pass: Process all collections to create nodes
  processCollections(collections, nodes);

  // Second pass: Handle reverse relationships
  processReverseRelationships(nodes);

  // Third pass: Create links based on all subcollection relationships
  createLinks(nodes, links);
  // Debugging breakpoint to inspect the final graph data
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
  Object.entries(collections).forEach(
    ([collectionName, collectionData], index) => {
      console.log(`Processing collection: ${collectionName}`);
      // Extract table columns and subcollections for display
      const columns = [];
      const subcollections = [];

      if (collectionData.properties) {
        processProperties(
          collectionName,
          collectionData,
          columns,
          subcollections
        );
      }

      // Calculate height based on properties with more padding
      // Height is dynamic based on number of properties to ensure all are visible
      const height = Math.max(
        250,
        150 + (columns.length + subcollections.length) * 22
      );

      // Create the node object with visual coordinates and data properties
      nodes.push({
        id: collectionName,
        name: collectionName,
        columns: columns,
        subcollections: subcollections,
        tablePath: collectionData.table_path || "No path specified",
        uniqueProperties: collectionData.unique_properties || ["id"],
        // Position nodes in a grid pattern for initial visualization
        x: 150 + (index % 3) * 500,
        y: 150 + Math.floor(index / 3) * 400,
        width: 350,
        height: height,
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
 * Extract and categorize properties from a collection into table columns and relationships.
 *
 * Processes each property in a collection, categorizing them as either:
 * - Regular table columns (table_column type)
 * - Relationships to other collections (simple_join, compound, or cartesian_product types)
 *
 * @param {string} collectionName - The name of the collection being processed
 * @param {Object} collectionData - The collection data containing properties and metadata
 * @param {Array} columns - Output array to store column information (modified in-place)
 * @param {Array} subcollections - Output array to store relationship information (modified in-place)
 */
function processProperties(
  collectionName,
  collectionData,
  columns,
  subcollections
) {
  Object.entries(collectionData.properties).forEach(([propName, propData]) => {
    // Handle regular table columns (attributes)
    if (propData.type === "table_column") {
      columns.push({
        name: propName,
        columnName: propData.column_name,
        dataType: propData.data_type,
      });
    }
    // Handle relationship properties between collections
    else if (
      ["simple_join", "cartesian_product", "general_join"].includes(
        propData.type
      )
    ) {
      processRelationship(
        collectionName,
        propName,
        propData,
        collectionData,
        subcollections
      );
    }
  });
}

/**
 * Process a relationship property between collections and add it to subcollections.
 *
 * Handles different relationship types (simple_join, compound, cartesian_product)
 * and determines the target collection for each relationship. Creates a structured
 * representation of the relationship with all its attributes.
 *
 * @param {string} collectionName - The name of the source collection
 * @param {string} propName - The name of the relationship property
 * @param {Object} propData - The relationship property data with type and connection details
 * @param {Object} collectionData - The parent collection data containing all properties
 * @param {Array} subcollections - Array to store the processed relationship (modified in-place)
 */
function processRelationship(
  collectionName,
  propName,
  propData,
  collectionData,
  subcollections
) {
  let targetCollection;
  let condition = null; // Variable to store the condition for general_join

  try {
    // Handle different relationship types to determine the target collection
    if (propData.type === "simple_join") {
      // For simple joins, the target is directly specified
      targetCollection = propData.other_collection_name;
    } else if (propData.type === "general_join") {
      // For general joins, target is specified, and there's a condition
      targetCollection = propData.other_collection_name;
      condition = propData.condition; // Extract the condition string
    } else {
      // For cartesian product relationships
      targetCollection = propData.other_collection_name;
    }
  } catch (e) {
    console.error(`Error processing relationship ${propName}:`, e);
    targetCollection = "unknown";
  }

  // Only add subcollection if we have a valid target
  if (targetCollection && targetCollection !== "unknown") {
    console.log(
      `Adding subcollection: ${propName} (${propData.type}) -> ${targetCollection}`
    );

    // Create the relationship object with all needed properties for visualization
    subcollections.push({
      name: propName,
      type: propData.type,
      target: targetCollection,
      singular: propData.singular || false, // One-to-one relationship flag
      noCollisions: propData.no_collisions || false, // Indicates if duplicate relations are prevented
      primary: propData.primary_property, // Primary join property
      condition: condition, // Add the condition field (will be null for non-general joins)
      propertyData: propData, // Store the original property data for reference
      isForward: true, // Flag this as a forward relationship (vs reverse relationships created later)
    });
  }
}

/**
 * Create bidirectional relationships by adding reverse relationships to target nodes.
 *
 * For each forward relationship from collection A to B, this function creates
 * a corresponding reverse relationship from B to A, ensuring proper navigation
 * in both directions. Handles relationship cardinality appropriately.
 *
 * @param {Array} nodes - The array of all node objects with their subcollections
 */
function processReverseRelationships(nodes) {
  nodes.forEach((sourceNode) => {
    sourceNode.subcollections.forEach((subcollection) => {
      const targetNode = nodes.find((n) => n.id === subcollection.target);
      if (targetNode && subcollection.propertyData?.reverse_relationship_name) {
        console.log(
          `Adding reverse relationship from ${targetNode.id} to ${sourceNode.id}`
        );

        // Transform the condition for the reverse relationship if it exists
        let reverseCondition = subcollection.condition;
        if (reverseCondition) {
          // Use placeholders to safely swap 'self.' and 'other.'
          const tempSelf = "__TEMP_SELF__";
          reverseCondition = reverseCondition.replace(/self\./g, tempSelf);
          reverseCondition = reverseCondition.replace(/other\./g, "self.");
          reverseCondition = reverseCondition.replace(
            new RegExp(tempSelf, "g"),
            "other."
          );
        }

        targetNode.subcollections.push({
          name: subcollection.propertyData.reverse_relationship_name,
          type: subcollection.type, // Keep the same type
          target: sourceNode.id,
          singular: !subcollection.singular, // Reverse cardinality (approximate)
          noCollisions: subcollection.noCollisions, // Maintain collision property
          primary: subcollection.primary, // Reverse relationships might not have a clear primary property
          condition: reverseCondition, // Use the transformed condition
          propertyData: { original_relationship: subcollection.name }, // Reference original
          isForward: false, // This is a reverse relationship
        });
      }
    });
  });
}

/**
 * Create graph links based on the relationships between collections.
 *
 * Transforms the subcollection relationships stored in each node into visible
 * links for the graph visualization. Each link contains detailed information
 * about the relationship type, cardinality, and direction.
 *
 * @param {Array} nodes - The array of all node objects
 * @param {Array} links - The array to store the created link objects (modified in-place)
 */
function createLinks(nodes, links) {
  nodes.forEach((sourceNode) => {
    sourceNode.subcollections.forEach((subcollection) => {
      const targetNode = nodes.find((n) => n.id === subcollection.target);
      if (targetNode) {
        console.log(
          `Creating link: ${sourceNode.id} -> ${targetNode.id} (${subcollection.type})`
        );
        links.push({
          source: sourceNode.id,
          target: targetNode.id,
          type: subcollection.type,
          singular: subcollection.singular,
          name: subcollection.name, // Add the relationship name to the link
          condition: subcollection.condition, // Add the condition to the link data
        });
      }
    });
  });
}

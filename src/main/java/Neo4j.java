
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

enum MyRel implements RelationshipType {
	VILLE_NAISSANCE, PAYS_NAISSANCE, PROFRESSION_AVANT_MIGRATION, PROFESSION_DURANT_INTERROGATORE, PERSONNE_ADM, LIEN_ALIAS,
	LIEN_PROSTITUTION, PARLER, PERSONNE_ROLE, LIEN_TELEPHONE, LIEN_FAMILIAUX, PERSONNE_COTE, PERSONNE_LOCALISATION, ADM_PAYS_TRANSIT1,
	ADM_PAYS_TRANSIT2, LIEU_PROSTITUTION, LOCALISATION_PAYS, LOCALISATION_COUPLE, LOCALISATION_VILLE, SIMILAIRE, LOCALISATION_EGO, LOCALISATION_ALTER,
	INTERMEDIAIRE1, INTERMEDIAIRE2, PROSTITUTION_COTE,  LOCALISATION_CEREMONIE, RELATION_COTE, REL_PERSONNE_ALTER, REL_PERSONNE_EGO, REL_COTE,
	REL_SOUTIEN, REL_SANG, REL_FINANCIER, REL_RESEAU, REL_JUJU, REL_CONNAISSANCE, REL_SEXUEL, REL_INCONNU, LIEN_SOUTIEN, LIEN_SANG, LIEN_FINANCIER,
	LIEN_RESEAU, LIEN_JUJU, LIEN_AUTRE, LIEN_SEXUEL, LIEN_INCONNU, LIEN_CONNAISSANCE
	}

public class Neo4j {
	private String DBPATH = "/Users/hren/Downloads/DBTest/GTest";
	private GraphDatabaseService graphDb;
	private int sumNodes;
	private int sumEdges;
	private SQLReader myReader;
	private Map<String, Map<String, Object>> tables;
	Label personne = DynamicLabel.label("Personne");
	Label relation = DynamicLabel.label("Relation");
	Label telephone = DynamicLabel.label("Telephone");
	Label role = DynamicLabel.label("Role");
	Label langue = DynamicLabel.label("Langue");
	Label ville = DynamicLabel.label("Ville");
	Label familiaux = DynamicLabel.label("Familiaux");
	Label lieuProstitution = DynamicLabel.label("LieuProstitution");
	Label localisation = DynamicLabel.label("Localisation");
	Label pays = DynamicLabel.label("Pays");
	Label administratifs = DynamicLabel.label("Administratifs");
	Label cote = DynamicLabel.label("Cote");
	Label alias = DynamicLabel.label("Alias");
	Label juju = DynamicLabel.label("Juju");
	Label financier = DynamicLabel.label("Financier");
	Label reseau = DynamicLabel.label("Reseau");
	Label sexuel = DynamicLabel.label("Sexuel");
	Label sang = DynamicLabel.label("Sang");
	Label soutien = DynamicLabel.label("Soutien");
	Label connaissance = DynamicLabel.label("Connaissance");

	private Map<String, String> fonctionJuju;
	private Map<String, String> natureCote;
	private Map<String, String> actionReseau;
	private Map<String, String> nationalite;
	private Map<String, String> modalite;
	private Map<String, String> profession;
	private Map<String, String> frequenceFluxFinancier;
	private Map<String, String> contexteSocioGeo;
	private Map<String, String> actionEnContrepartie;
	private Map<String, String> typeSoutien;
	private IndexMap lienJuju = new IndexMap();
	private IndexMap lienReseau = new IndexMap();
	private IndexMap lienSoutien = new IndexMap();
	private IndexMap lienFinancier = new IndexMap();
	private Map<String, IndexMap> indexForLien = new HashMap<String, IndexMap>();
	public Neo4j( SQLReader reader) {
		myReader = reader;
		tables = new HashMap<String, Map<String, Object>>();
		graphDb = new GraphDatabaseFactory().newEmbeddedDatabase( DBPATH );
		registerShutdownHook( graphDb );
	}
	
	public void startImport () {
		try {
			Transaction tx = graphDb.beginTx();
			importNodes();
			createRelationship();
//			createRelsRelation("st");
			tx.success();
		} catch (Exception e) {
			System.out.println( "Import faild" );
		}
	}

	private void importNodes() {
		fonctionJuju = createIndexMap("fonctionJuju", "IDFonctionJuju", "FonctionJuju");
		natureCote = createIndexMap("natureCote", "IDNatureCote", "NatureCote");
		actionReseau = createIndexMap("actionReseau", "IDActionReseau", "ActionReseau");
		nationalite = createIndexMap("nationalite", "IDNationalite", "Nationalite");
		modalite = createIndexMap("modalite", "IDModalite", "Modalite");
		profession = createIndexMap("profession", "IDProfession", "Profession");
		frequenceFluxFinancier = createIndexMap("frequenceFluxFinancier", "IDFrequence", "Frequence");
		contexteSocioGeo = createIndexMap("contexteSocioGeo", "IDContexteSocioGeo", "ContexteSocioGeo");
		actionEnContrepartie = createIndexMap("actionEnContrepartie", "IDActionEnContrepartie", "ActionEnContrepartie");
		typeSoutien = createIndexMap("typeSoutien", "IDTypeSoutien", "TypeSoutien");
		
		importPersonne();//274 nodes
		importSang(); //51 nodes
		importCote();//142 nodes
		importVille();//78 nodes
		importPays();// 15 nodes
		importAttributsAdministratifs();//41 nodes time format error
		importLocalisation();// 155 nodes
		importLanguage();//11 nodes
		importRole();//11 nodes
		importTelephone();//101 nodes
		importAlias(); // 116 nodes
		importAttributsFamiliaux();//31 nodes
		importSexuel();// 0 nodes
		importJuju();//0 nodes
		importReseau();//350 nodes
		importLieuPrositution();// 0 nodes
		importSoutien();// 0 nodes
		importConnaissance();// 0 nodes
		importFinancier();// 189 nodes
		importRelation();
	}
	
	private void createRelationship() {
		createRelsRelation();
		createRelForPersonne();
		createRelForAdmin();
		createRelForLocalisation();
		createRelForFamillaux();
		createRelForFinancier();
		createRelForLieuProstitution();
		createRelForReseau();
		createRelForConnaissance();
		createRelJuju();
		createRelThreeTables("personne", "alias", "personneToAlias", "IDPersonne", "IDAlias", MyRel.LIEN_ALIAS, null);
		createRelThreeTables("personne", "telephone", "personneToTelephone", "IDPersonne", "IDTelephone", MyRel.LIEN_TELEPHONE, null);
		createRelThreeTables("personne", "langue", "personneToLangue", "IDPersonne", "IDLangue", MyRel.PARLER, null);
		String[] propRole = new String[4];
		propRole[0] = "DebutRole";
		propRole[1] = "PeriodeMois";
		propRole[2] = "FinRole";
		propRole[3] = "IdentifiantQuali";
		createRelThreeTables("personne", "role", "personneToRole", "IDPersonne", "IDRole", MyRel.PERSONNE_ROLE, propRole);
		createRelThreeTables("personne", "localisation", "personneToLocalisation", "IDPersonne", "IDLocalisation", MyRel.PERSONNE_LOCALISATION, null);
		createRelThreeTables("personne", "cote", "personneToCote", "IDPersonne", "IDCote", MyRel.PERSONNE_COTE, null);
		createRelThreeTables("relation", "cote", "relationToCote", "IDRelation", "IDCote", MyRel.RELATION_COTE, null);
		createRelThreeTables("personne", "personne", "possibiliteSimilaire", "IDPersonneMajeure", "IDPersonneMineure", MyRel.SIMILAIRE, null);
	}

	private void importPersonne() {
		String tableName = "personne";
		String indexName = "IDPersonne";
		String[] items = {"IDTmp", "IDDossier", "TypePersonne", "Sexe", "Nom", "Prenom", "DateNaissance", "SeProstitue",
				"DetteInitiale", "DetteRenegociee", "DateDettePayee", "DateEstRecrute", "DateRecrute", "Diplome"};
		ArrayList<Label> labels = new ArrayList<Label>();
		labels.add(personne);
		Map<String, Map<String, String>> props = new HashMap<String, Map<String, String>>();
		props.put("IDNationalite", nationalite);
		props.put("IDProfessionAvantMigration", profession);
		props.put("IDProfessionDurantInterrogatoire", profession);
		createNodes(tableName, labels, indexName, items, props);
	}
	
	private void importRelation() {
		String tableName = "relation";
		String indexName = "IDRelation";
		String[] items = {"IDTmp", "TraceLienDossier"};
		ArrayList<Label> labels = new ArrayList<Label>();
		labels.add(relation);
		Map<String, Map<String, String>> props = new HashMap<String, Map<String, String>>();
		props.put("IDContexteSocioGeo", contexteSocioGeo);
		createNodes(tableName, labels, indexName, items, props);
	}
	
	private void createRelForPersonne() {
		String tableName = "personne";
		String indexName = "IDPersonne";
		Map<String, String> indexMap = new HashMap<String, String>();
		Map<String, RelationshipType> rels = new HashMap<String, RelationshipType>();
		indexMap.put("IDVilleNaissance", "ville");
		rels.put("IDVilleNaissance", MyRel.VILLE_NAISSANCE);
		indexMap.put("IDPaysNaissance", "pays");
		rels.put("IDPaysNaissance", MyRel.PAYS_NAISSANCE);
		createRelsOneTable(tableName, indexName, indexMap, rels);
		createRelLocal("personne", "attributsAdministratifs", MyRel.PERSONNE_ADM); // 41 edges;
		createRelLocal("personne", "lieuProstitution", MyRel.LIEN_PROSTITUTION);
		createRelLocal("personne", "attributsFamiliaux", MyRel.LIEN_FAMILIAUX);
	}

	private void createRelForAdmin() {
		String tableName = "attributsAdministratifs";
		String indexName = "IDPersonneAdm";
		Map<String, String> indexMap = new HashMap<String, String>();
		Map<String, RelationshipType> rels = new HashMap<String, RelationshipType>();
		indexMap.put("IDPaysTransit1", "pays");
		rels.put("IDPaysTransit1", MyRel.ADM_PAYS_TRANSIT1);
		indexMap.put("IDPaysTransit2", "pays");
		rels.put("IDPaysTransit2", MyRel.ADM_PAYS_TRANSIT2);
		createRelsOneTable(tableName, indexName, indexMap, rels);
	}
	
	private void createRelForLocalisation() {
		String tableName = "localisation";
		String indexName = "IDLocalisation";
		Map<String, String> indexMap = new HashMap<String, String>();
		Map<String, RelationshipType> rels = new HashMap<String, RelationshipType>();
		indexMap.put("IDPays", "pays");
		rels.put("IDPays", MyRel.LOCALISATION_PAYS);
		indexMap.put("IDVille", "ville");
		rels.put("IDVille", MyRel.LOCALISATION_VILLE);
		createRelsOneTable(tableName, indexName, indexMap, rels);
	}
	
	private void createRelJuju() {
		String tableName = "lienJuju";
		String indexName = "IDLienJuju";
		Map<String, String> indexMap = new HashMap<String, String>();
		Map<String, RelationshipType> rels = new HashMap<String, RelationshipType>();
		indexMap.put("IDLocalisationCeremonie", "localisation");
		rels.put("IDLocalisationCeremonie", MyRel.LOCALISATION_CEREMONIE);
		createRelsOneTable(tableName, indexName, indexMap, rels);
	}

	private void createRelForConnaissance() {
		String tableName = "lienConnaissance";
		String indexName = "IDRelation";
		Map<String, String> indexMap = new HashMap<String, String>();
		Map<String, RelationshipType> rels = new HashMap<String, RelationshipType>();
		indexMap.put("IDLocalisationEgo", "localisation");
		rels.put("IDLocalisationEgo", MyRel.LOCALISATION_EGO);
		indexMap.put("IDLocalisationAlter", "localisation");
		rels.put("IDLocalisationAlter", MyRel.LOCALISATION_ALTER);
		createRelsOneTable(tableName, indexName, indexMap, rels);
	}
	
	private void createRelForReseau() {
		String tableName = "lienReseau";
		String indexName = "IDLienReseau";
		Map<String, String> indexMap = new HashMap<String, String>();
		Map<String, RelationshipType> rels = new HashMap<String, RelationshipType>();
		indexMap.put("IDLocalisationEgo", "localisation");
		rels.put("IDLocalisationEgo", MyRel.LOCALISATION_EGO);
		indexMap.put("IDLocalisationAlter", "localisation");
		rels.put("IDLocalisationAlter", MyRel.LOCALISATION_ALTER);
		createRelsOneTable(tableName, indexName, indexMap, rels);
	}
	
	private void createRelForFamillaux() {
		String tableName = "attributsFamiliaux";
		String indexName = "IDPersonneFam";
		Map<String, String> indexMap = new HashMap<String, String>();
		Map<String, RelationshipType> rels = new HashMap<String, RelationshipType>();
		indexMap.put("IDLocalisationCouple", "localisation");
		rels.put("IDLocalisationCouple", MyRel.LOCALISATION_COUPLE);
		createRelsOneTable(tableName, indexName, indexMap, rels);
	}
	
	private void createRelForFinancier() {
		String tableName = "lienFinancier";
		String indexName = "IDLienFinancier";
		Map<String, String> indexMap = new HashMap<String, String>();
		Map<String, RelationshipType> rels = new HashMap<String, RelationshipType>();
		indexMap.put("IDIntermediaire", "personne");
		rels.put("IDIntermediaire", MyRel.INTERMEDIAIRE1);
		indexMap.put("IDIntermediaire2", "personne");
		rels.put("IDIntermediaire2", MyRel.INTERMEDIAIRE2);
		indexMap.put("IDLocalisationEgo", "localisation");
		rels.put("IDLocalisationEgo", MyRel.LOCALISATION_EGO);
		indexMap.put("IDLocalisationAlter", "localisation");
		rels.put("IDLocalisationAlter", MyRel.LOCALISATION_ALTER);
		createRelsOneTable(tableName, indexName, indexMap, rels);
	}

	private void createRelForLieuProstitution() {
		String tableName = "lieuProstitution";
		String indexName = "IDPersonne";
		Map<String, String> indexMap = new HashMap<String, String>();
		Map<String, RelationshipType> rels = new HashMap<String, RelationshipType>();
		indexMap.put("IDSource", "cote");
		rels.put("IDSource", MyRel.PROSTITUTION_COTE);
		indexMap.put("IDLocalisation", "localisation");
		rels.put("IDLocalisation", MyRel.LIEU_PROSTITUTION);
		createRelsOneTable(tableName, indexName, indexMap, rels);
	}

	private void importCote() {
		String tableName = "cote";
		String indexName = "IDCote";
		String[] items = {"NomCote", "DateCote", "InformationsNonExploitees"};
		ArrayList<Label> labels = new ArrayList<Label>();
		labels.add(cote);
		Map<String, Map<String, String>> props = new HashMap<String, Map<String, String>>();
		props.put("IDNatureCote", natureCote);
		createNodes(tableName, labels, indexName, items, props);
	}

	private void importLieuPrositution() {
		String tableName = "lieuProstitution";
		String indexName = "IDPersonne";
		String[] items = {};
		ArrayList<Label> labels = new ArrayList<Label>();
		labels.add(lieuProstitution);
		createNodes(tableName, labels, indexName, items, null);
	}

	private void importPays() {
		String tableName = "pays";
		String indexName = "IDPays";
		String[] items = {"Pays"};
		ArrayList<Label> labels = new ArrayList<Label>();
		labels.add(pays);
		createNodes(tableName, labels, indexName, items, null);
	}

	private void importAlias() {
		String tableName = "alias";
		String indexName = "IDAlias";
		String[] items = {"Alias"};
		ArrayList<Label> labels = new ArrayList<Label>();
		labels.add(alias);
		createNodes(tableName, labels, indexName, items, null);
	}

	private void importVille() {
		String tableName = "ville";
		String indexName = "IDVille";
		String[] items = {"Ville"};
		ArrayList<Label> labels = new ArrayList<Label>();
		labels.add(ville);
		createNodes(tableName, labels, indexName, items, null);
	}

	private void importLanguage() {
		String tableName = "langue";
		String indexName = "IDLangue";
		String[] items = {"Langue"};
		ArrayList<Label> labels = new ArrayList<Label>();
		labels.add(langue);
		createNodes(tableName, labels, indexName, items, null);
	}

	private void importRole() {
		String tableName = "role";
		String indexName = "IDRole";
		String[] items = {"Role"};
		ArrayList<Label> labels = new ArrayList<Label>();
		labels.add(role);
		createNodes(tableName, labels, indexName, items, null);
	}

	private void importTelephone() {
		String tableName = "telephone";
		String indexName = "IDTelephone";
		String[] items = {"NumTelephone"};
		ArrayList<Label> labels = new ArrayList<Label>();
		labels.add(telephone);
		createNodes(tableName, labels, indexName, items, null);
	}

	private void importLocalisation() {
		String tableName = "localisation";
		String indexName = "IDLocalisation";
		String[] items = {"Adresse", "CodePostal"};
		ArrayList<Label> labels = new ArrayList<Label>();
		labels.add(localisation);
		createNodes(tableName, labels, indexName, items, null);
	}

	private void importAttributsFamiliaux() {
		String tableName = "attributsFamiliaux";
		String indexName = "IDPersonneFam";
		String[] items = {"Pere", "Mere", "RuptureParentale", "Fratrie", "PositionFratrie",
						"SituationMatrimoniale", "ValidationSource", "VitEnCouple", "Enceinte"};
		ArrayList<Label> labels = new ArrayList<Label>();
		labels.add(familiaux);
		createNodes(tableName, labels, indexName, items, null);
	}

	private void importAttributsAdministratifs() {
		String tableName = "attributsAdministratifs";
		String indexName = "IDPersonneAdm";
		String[] items = {"NumPassport", "DebutValPassport", "FinValPassport", "NumRecepisse", "NumRecoursOFPRA", "DebutValRecepisse", "FinValRecepisse",
							"NumOQTF", "DebutOQTF", "FinOQTF", "NumSejour", "DebutValSejour", "FinValSejour", "PrestationSociale",
							"ModeMigration", "ArriveeEurope", "ArriveeFrance"};
		ArrayList<Label> labels = new ArrayList<Label>();
		labels.add(administratifs);
		Map<String, Map<String, String>> props = new HashMap<String, Map<String, String>>();
		props.put("IDNationalitePassport", nationalite);
		createNodes(tableName, labels, indexName, items, props);
	}
	
	private void importSexuel() {
		String tableName = "lienSexuel";
		String indexName = "IDRelation";
		String[] items = {"Prostitution", "Viol", "EnCouple", "DateDebut", "DateFin", "TypeLienSexuel"};
		ArrayList<Label> labels = new ArrayList<Label>();
		labels.add(sexuel);
		createNodes(tableName, labels, indexName, items, null);
	}

	private void importSoutien() {
		String tableName = "lienSoutien";
		String indexName = "IDLienSoutien";
		String[] items = {"DatePremierContact", "Intermediaire", "IDSoutien"};
		ArrayList<Label> labels = new ArrayList<Label>();
		labels.add(soutien);
		Map<String, Map<String, String>> props = new HashMap<String, Map<String, String>>();
		props.put("IDTypeSoutien", typeSoutien);
		createNodes(tableName, labels, indexName, items, props, lienSoutien);
	}

	private void importSang() {
		String tableName = "lienSang";
		String indexName = "IDRelation";
		String[] items = {"Type", "Certification"};
		ArrayList<Label> labels = new ArrayList<Label>();
		labels.add(sang);
		createNodes(tableName, labels, indexName, items, null);
	}

	private void importFinancier() {
		String tableName = "lienFinancier";
		String indexName = "IDLienFinancier";
		String[] items = {"DateFlux", "MontantEuro", "Intermediaire", "IDFlux", "ActionDuFlux"};
		ArrayList<Label> labels = new ArrayList<Label>();
		labels.add(financier);
		Map<String, Map<String, String>> props = new HashMap<String, Map<String, String>>();
		props.put("IDActionEnContrepartie", actionEnContrepartie);
		props.put("IDFrequence", frequenceFluxFinancier);
		props.put("IDModalite", modalite);
		props.put("IDModalite2", modalite);
		createNodes(tableName, labels, indexName, items, props, lienFinancier);
	}

	private void importReseau() {
		String tableName = "lienReseau";
		String indexName = "IDLienReseau";
		String[] items = {"DateIdentification","Intermediaire", "IDReseau", "NoteAction"};
		ArrayList<Label> labels = new ArrayList<Label>();
		labels.add(reseau);
		Map<String, Map<String, String>> props = new HashMap<String, Map<String, String>>();
		props.put("IDActionReseau", actionReseau);
		createNodes(tableName, labels, indexName, items, props, lienReseau);
	}

	private void importJuju() {
		String tableName = "lienJuju";
		String indexName = "IDLienJuju";
		String[] items = {"Date", "IDJuju"};
		ArrayList<Label> labels = new ArrayList<Label>();
		labels.add(juju);
		Map<String, Map<String, String>> props = new HashMap<String, Map<String, String>>();
		props.put("IDFonctionAlterJuju", fonctionJuju);
		props.put("IDFonctionEgoJuju", fonctionJuju);
		createNodes(tableName, labels, indexName, items, props, lienJuju);
	}

	private void importConnaissance() {
		String tableName = "lienConnaissance";
		String indexName = "IDRelation";
		String[] items = {"PremierEvenement"};
		ArrayList<Label> labels = new ArrayList<Label>();
		labels.add(connaissance);
		createNodes(tableName, labels, indexName, items, null);
	}

	private Map<String, String> createIndexMap(String tableName, String indexKey, String valueTitle) {
		Map<String, String> tempMap = new HashMap<String, String>();
		try {
			String sqlCommand = "SELECT * FROM " + tableName;
			ResultSet rs = myReader.excute( sqlCommand );
			int sumCount = 0;
			while ( rs.next() ) {
				String key = getValueFromResultSet(rs, indexKey);
				String value = getValueFromResultSet(rs, valueTitle);
				tempMap.put(key, value);
				sumCount += 1;
			}
			System.out.println("Successfully get " + sumCount + " values from " + tableName + ".");
			System.out.println("*************************");
			rs.close();
		} catch (Exception e) {
			System.out.println("Import indexMap " + tableName + " faild.");
		}
		return tempMap;
	}

	private void createRelLocal(String table1, String table2, RelationshipType edgeLabel) {
		Map<String, Object> t1 = tables.get(table1);
		Map<String, Object> t2 = tables.get(table2);
		Node node1;
		Node node2;
		sumEdges = 0;
		for (String key : t1.keySet()) {
			node2 = (Node) t2.get(key);
			if (node2 != null) {
				node1 = (Node) t1.get(key);
				sumEdges += 1;
				node1.createRelationshipTo( node2, edgeLabel);
			}
		}
		System.out.println("Successfully created " + sumEdges + " edges.");
		Main.edgesNum += sumEdges;
		System.out.println("*************************");
	}

	private void createRelsRelation() {
		try {
			String sqlCommand = "SELECT * FROM relation";
			ResultSet rs = myReader.excute( sqlCommand );
			sumEdges = 0;
			Node node1;
			Node node2;
			Node rela;
			while ( rs.next() ) {
				String typeLien = getValueFromResultSet(rs, "TypeLien");
				String indexStr1 = getValueFromResultSet(rs, "IDEgo");
				String indexStr2 = getValueFromResultSet(rs, "IDAlter");
				String indexRel = getValueFromResultSet(rs, "IDRelation");
				try {
					node1 = (Node) tables.get("personne").get(indexStr1);
					node2 = (Node) tables.get("personne").get(indexStr2);
					rela = (Node) tables.get("relation").get(indexRel);
					node1.createRelationshipTo(rela, MyRel.REL_PERSONNE_ALTER);
					rela.createRelationshipTo(node2, MyRel.REL_PERSONNE_EGO);
					sumEdges += 3;
					String tableName = "null";
					RelationshipType myRel = MyRel.REL_INCONNU;
					RelationshipType p2p = MyRel.LIEN_INCONNU;
					switch (typeLien) {
						case "financier":
							tableName = "lienFinancier";
							myRel = MyRel.REL_FINANCIER;
							p2p = MyRel.LIEN_FINANCIER;
							break;
						case "juju":
							tableName= "lienJuju";
							myRel = MyRel.REL_JUJU;
							p2p = MyRel.LIEN_JUJU;
							break;
						case "réseau":
							tableName = "lienReseau";
							myRel = MyRel.REL_RESEAU;
							p2p = MyRel.LIEN_RESEAU;
							break;
						case "soutien": 
							tableName = "lienSoutien";
							myRel = MyRel.REL_SOUTIEN;
							myRel = MyRel.LIEN_SOUTIEN;
							break;
						case "sang":
							tableName = "lienSang";
							myRel = MyRel.REL_SANG;
							p2p = MyRel.LIEN_SANG;
							break;
						case "sexuel":
							tableName = "lienSexuel";
							myRel = MyRel.REL_SEXUEL;
							p2p = MyRel.LIEN_SEXUEL;
							break;
						case "connaissance":
							tableName = "lienConnaissance";
							myRel = MyRel.REL_CONNAISSANCE;
							p2p = MyRel.LIEN_CONNAISSANCE;
							break;
						case "autre":
							p2p = MyRel.LIEN_AUTRE;
							break;
						case "inconnu":
							p2p = MyRel.LIEN_INCONNU;
							break;
					}
					node1.createRelationshipTo(node2, p2p);
					switch (tableName) {
						case "lienSang":
						case "lienSexuel":
						case "lienConnaissance":
							Node node3 = (Node) tables.get(tableName).get(indexRel);
							if (node3 != null) {
								rela.createRelationshipTo(node3, myRel);
								sumEdges += 1;
							} else {
								System.out.println("relation " + indexRel + " hasn't imported into table " + tableName + ".");
							}
							break;
						case "lienJuju":
						case "lienReseau":
						case "lienSoutien":
						case "lienFinancier":
							ArrayList<String> relIndex = indexForLien.get(tableName).getValues(indexRel);
							if (relIndex != null) {
								for (String tempIndex : relIndex) {
									Node node4 = (Node) tables.get(tableName).get(tempIndex);
									rela.createRelationshipTo(node4, myRel);
									sumEdges += 1;
								}
							} else {
								System.out.println("relation " + indexRel + " hasn't imported into table " + tableName + ".");
							}
							break;
					}
				} catch (Exception e) {
					System.out.println("Error when indexing the relation between personne");
				}
			}
			System.out.println("Successfully created " + sumEdges + " edges.");
			Main.edgesNum += sumEdges;
			System.out.println("*************************");
			rs.close();
		} catch (Exception e) {
			System.out.println("Import relationships faild in createRelsOneTable.");
		}
	}
	
	private void createRelsOneTable(String table1, String index, Map<String, String> indexMap, Map<String, RelationshipType> rels) {
		try {
			String sqlCommand = "SELECT * FROM " + table1;
			ResultSet rs = myReader.excute( sqlCommand );
			sumEdges = 0;
			Node node1;
			Node node2;
			while ( rs.next() ) {
				for (String oneIndex : indexMap.keySet()) {
					String indexStr1 = getValueFromResultSet(rs, oneIndex);
					if (!indexStr1.equals("null")) {
						String indexStr = getValueFromResultSet(rs, index);
						String tableName = indexMap.get(oneIndex);
						node1 = (Node) tables.get(table1).get(indexStr);
						node2 = (Node) tables.get(tableName).get(indexStr1);
						if (node1 != null && node2 != null) {
							node1.createRelationshipTo(node2, rels.get(oneIndex));
							sumEdges += 1;
						} else {
							System.out.println("Wrong node indexing for creating index in createRelsOneTable in " + table1 + ".");
						}
					}
				}
			}
			System.out.println("Successfully created " + sumEdges + " edges.");
			Main.edgesNum += sumEdges;
			System.out.println("*************************");
			rs.close();
		} catch (Exception e) {
			System.out.println("Import relationships faild in createRelsOneTable.");
		}
	}
	
	private void createRelThreeTables(String table1, String table2, String tableInter, String index1, String index2, RelationshipType rel, String[] repops) {
		try {
			String sqlCommand = "SELECT * FROM " + tableInter;
			ResultSet rs = myReader.excute( sqlCommand );
			sumEdges = 0;
			while ( rs.next() ) {
				String indexStr1 = getValueFromResultSet(rs, index1);
				String indexStr2 = getValueFromResultSet(rs, index2);
				if (!indexStr2.equals("null") && !indexStr1.equals("null")) {
					Node node1 = (Node) tables.get(table1).get(indexStr1);
					Node node2 = (Node) tables.get(table2).get(indexStr2);
					if (node1 != null && node2 != null) {
						Relationship tempRela = node1.createRelationshipTo(node2, rel);
						if (repops != null) {
							for (String repop : repops) {
								String repoValue = getValueFromResultSet(rs, repop);
								tempRela.setProperty(repop, repoValue);
							}
						}
					} else {
						System.out.println("Error indexing for indexing nodes in RelThreeTables");
					}
					sumEdges += 1;
				} else {
					System.out.println("Error indexing for creatingRelThreeTables");
				}
			}
			System.out.println("Successfully created " + sumEdges + " edges.");
			Main.edgesNum += sumEdges;
			System.out.println("*************************");
			rs.close();
		} catch (Exception e) {
			System.out.println("Import 3 tables intermedie relationship faild.");
		}
	}

	private void createNodes( String tableName, ArrayList<Label> labels, String indexName, String[] items, Map<String, Map<String, String>> readProps) {
		try {
			Map<String, Object> oneTable = new HashMap<String, Object>();
			String sqlCommand = "SELECT * FROM " + tableName;
			ResultSet rs = myReader.excute( sqlCommand );
			String oneTitle;
			int itNums = items.length;
			sumNodes = 0;
			while ( rs.next() ) {
				Node tempNode = graphDb.createNode();
				for (Label oneLabel : labels) {
					tempNode.addLabel(oneLabel);
				}
				String indexStr = getValueFromResultSet(rs, indexName);
				for ( int j = 0; j < itNums; j += 1 ) {
					oneTitle = items[j];
					String strValue = getValueFromResultSet( rs, oneTitle );
					tempNode.setProperty( oneTitle, strValue );
				}
				if (readProps != null) {
					for (String key : readProps.keySet()) {
						String keyValue = getValueFromResultSet( rs, key );
						String prop = "null";
						if (keyValue != "null") {
							prop = readProps.get(key).get(keyValue);
						}
						tempNode.setProperty(key, prop);
					}
				}
				oneTable.put(indexStr, tempNode);
				sumNodes += 1;
			}
			tables.put(tableName, oneTable);
			System.out.println("Successfully imported " + sumNodes + " nodes.");
			Main.nodesNum += sumNodes;
			System.out.println("*************************");
			rs.close();
		} catch (Exception e) {
			System.out.println("read table faild");
		}
	}
	
	private void createNodes( String tableName, ArrayList<Label> labels, String indexName, String[] items, Map<String, Map<String, String>> readProps, IndexMap indexMap) {
		try {
			Map<String, Object> oneTable = new HashMap<String, Object>();
			String sqlCommand = "SELECT * FROM " + tableName;
			ResultSet rs = myReader.excute( sqlCommand );
			String oneTitle;
			int itNums = items.length;
			sumNodes = 0;
			while ( rs.next() ) {
				Node tempNode = graphDb.createNode();
				for (Label oneLabel : labels) {
					tempNode.addLabel(oneLabel);
				}
				String indexStr = getValueFromResultSet(rs, indexName);
				String indexRel = getValueFromResultSet(rs, "IDRelation");
				indexMap.addValues(indexRel, indexStr);
				for ( int j = 0; j < itNums; j += 1 ) {
					oneTitle = items[j];
					String strValue = getValueFromResultSet( rs, oneTitle );
					tempNode.setProperty( oneTitle, strValue );
				}
				if (readProps != null) {
					for (String key : readProps.keySet()) {
						String keyValue = getValueFromResultSet( rs, key );
						String prop = "null";
						if (keyValue != "null") {
							prop = readProps.get(key).get(keyValue);
						}
						tempNode.setProperty(key, prop);
					}
				}
				oneTable.put(indexStr, tempNode);
				sumNodes += 1;
			}
			tables.put(tableName, oneTable);
			indexForLien.put(tableName, indexMap);
			System.out.println("Successfully imported " + sumNodes + " nodes.");
			Main.nodesNum += sumNodes;
			System.out.println("*************************");
			rs.close();
		} catch (Exception e) {
			System.out.println("read table faild");
		}
	}
	
	private String getValueFromResultSet( ResultSet rs, String title ) {
		String res = "test";
		try {
			Object item = rs.getObject(title);
			res = (item == null ? "null" : item.toString());
		} catch (Exception e) {
			System.out.println("Unknow value.");
			res = "null";
		}
		return res;
	}
	
	public void shutDownDB() {
		graphDb.shutdown();
	}
	
	private static void registerShutdownHook( final GraphDatabaseService graphDb ) {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				graphDb.shutdown();
			}
		});
	}
}

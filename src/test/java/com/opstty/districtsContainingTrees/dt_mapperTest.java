package com.opstty.districtsContainingTrees;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;


class mapperDistrictsContainingTreesTest {

    private Mapper.Context context;
    private mapperDistrictsContainingTrees mapper;

    @org.junit.jupiter.api.BeforeEach
    public void setUp() {
        this.mapper = new mapperDistrictsContainingTrees();
    }

    @org.junit.jupiter.api.Test
    public void testMap() throws IOException, InterruptedException{
        String value = "(48.857140829, 2.29533455314);7;Maclura;pomifera;Moraceae;1935;13.0;;Quai Branly, avenue de La Motte-Piquet, avenue de la Bourdonnais, avenue de Suffren;Oranger des Osages;;6;Parc du Champs de Mars";
        String value2 = "GEOPOINT;ARRONDISSEMENT;GENRE;ESPECE;FAMILLE;ANNEE PLANTATION;HAUTEUR;CIRCONFERENCE;ADRESSE;NOM COMMUN;VARIETE;OBJECTID;NOM_EV";
        String[] vals = value.toString().split(";+");

        Float height = Float.parseFloat(vals[6]);

        System.out.println(height);

        this.mapper.map(null, new Text(value), this.context);
    }
}
SQL> SELECT * FROM
  2  (SELECT B.ORIGINALTITLE, B.GENRES, R.AVERAGERATING
  3  FROM TITLE_BASICS B, TITLE_RATINGS R
  4  WHERE B.ENDYEAR BETWEEN '2001'AND '2005'
  5  AND B.GENRES LIKE '%Comedy%Romance%'
  6  AND B.TCONST=R.TCONST
  7  ORDER BY R.AVERAGERATING DESC)
  8  WHERE ROWNUM <=5;

ORIGINALTITLE                                                                   
--------------------------------------------------------------------------------
GENRES                                                                          
--------------------------------------------------------------------------------
AVERAGERATING                                                                   
-------------                                                                   
Friends                                                                         
Comedy,Romance                                                                  
          8.9                                                                   
                                                                                
Coupling                                                                        
Comedy,Romance                                                                  
          8.6                                                                   

ORIGINALTITLE                                                                   
--------------------------------------------------------------------------------
GENRES                                                                          
--------------------------------------------------------------------------------
AVERAGERATING                                                                   
-------------                                                                   
                                                                                
Eisai to tairi mou                                                              
Comedy,Romance                                                                  
          8.4                                                                   
                                                                                
As Time Goes By                                                                 
Comedy,Romance                                                                  

ORIGINALTITLE                                                                   
--------------------------------------------------------------------------------
GENRES                                                                          
--------------------------------------------------------------------------------
AVERAGERATING                                                                   
-------------                                                                   
          8.3                                                                   
                                                                                
Amor Descarado                                                                  
Comedy,Drama,Romance                                                            
          8.3                                                                   
                                                                                

SQL> 
SQL> SELECT * FROM
  2  (SELECT B.ORIGINALTITLE, B.GENRES, R.AVERAGERATING
  3  FROM TITLE_BASICS B, TITLE_RATINGS R
  4  WHERE B.ENDYEAR BETWEEN '2001'AND '2005'
  5  AND B.GENRES LIKE '%Comedy%Romance%'
  6  AND B.TCONST=R.TCONST
  7  ORDER BY R.AVERAGERATING)
  8  WHERE ROWNUM <=5;

ORIGINALTITLE                                                                   
--------------------------------------------------------------------------------
GENRES                                                                          
--------------------------------------------------------------------------------
AVERAGERATING                                                                   
-------------                                                                   
Al salir de clase                                                               
Comedy,Drama,Romance                                                            
          3.5                                                                   
                                                                                
Psie serce                                                                      
Comedy,Drama,Romance                                                            
          3.5                                                                   

ORIGINALTITLE                                                                   
--------------------------------------------------------------------------------
GENRES                                                                          
--------------------------------------------------------------------------------
AVERAGERATING                                                                   
-------------                                                                   
                                                                                
An thymitheis t' oneiro mou                                                     
Comedy,Romance                                                                  
            4                                                                   
                                                                                
Paco y Veva                                                                     
Comedy,Musical,Romance                                                          

ORIGINALTITLE                                                                   
--------------------------------------------------------------------------------
GENRES                                                                          
--------------------------------------------------------------------------------
AVERAGERATING                                                                   
-------------                                                                   
            4                                                                   
                                                                                
Para??so                                                                        
Comedy,Romance                                                                  
          4.7                                                                   
                                                                                

SQL> SELECT * FROM
  2  (SELECT B.ORIGINALTITLE, B.GENRES, R.AVERAGERATING
  3  FROM TITLE_BASICS B, TITLE_RATINGS R
  4  WHERE B.ENDYEAR BETWEEN '2001'AND '2005'
  5  AND B.GENRES LIKE '%Action%Thriller%'
  6  AND B.TCONST=R.TCONST
  7  ORDER BY R.AVERAGERATING DESC)
  8  WHERE ROWNUM <=5;

ORIGINALTITLE                                                                   
--------------------------------------------------------------------------------
GENRES                                                                          
--------------------------------------------------------------------------------
AVERAGERATING                                                                   
-------------                                                                   
C??digo negro                                                                   
Action,Thriller                                                                 
          8.4                                                                   
                                                                                
Fastlane                                                                        
Action,Crime,Thriller                                                           
          7.5                                                                   

ORIGINALTITLE                                                                   
--------------------------------------------------------------------------------
GENRES                                                                          
--------------------------------------------------------------------------------
AVERAGERATING                                                                   
-------------                                                                   
                                                                                
Tom Stone                                                                       
Action,Drama,Thriller                                                           
          7.3                                                                   
                                                                                
Hamilton                                                                        
Action,Thriller                                                                 

ORIGINALTITLE                                                                   
--------------------------------------------------------------------------------
GENRES                                                                          
--------------------------------------------------------------------------------
AVERAGERATING                                                                   
-------------                                                                   
          6.4                                                                   
                                                                                
Spetsnaz II                                                                     
Action,Adventure,Thriller                                                       
          6.2                                                                   
                                                                                

SQL> SELECT * FROM
  2  (SELECT B.ORIGINALTITLE, B.GENRES, R.AVERAGERATING
  3  FROM TITLE_BASICS B, TITLE_RATINGS R
  4  WHERE B.ENDYEAR BETWEEN '2001'AND '2005'
  5  AND B.GENRES LIKE '%Action%Thriller%'
  6  AND B.TCONST=R.TCONST
  7  ORDER BY R.AVERAGERATING)
  8  WHERE ROWNUM <=5;

ORIGINALTITLE                                                                   
--------------------------------------------------------------------------------
GENRES                                                                          
--------------------------------------------------------------------------------
AVERAGERATING                                                                   
-------------                                                                   
The Immortal                                                                    
Action,Horror,Thriller                                                          
          5.1                                                                   
                                                                                
Spetsnaz II                                                                     
Action,Adventure,Thriller                                                       
          6.2                                                                   

ORIGINALTITLE                                                                   
--------------------------------------------------------------------------------
GENRES                                                                          
--------------------------------------------------------------------------------
AVERAGERATING                                                                   
-------------                                                                   
                                                                                
Hamilton                                                                        
Action,Thriller                                                                 
          6.4                                                                   
                                                                                
Tom Stone                                                                       
Action,Drama,Thriller                                                           

ORIGINALTITLE                                                                   
--------------------------------------------------------------------------------
GENRES                                                                          
--------------------------------------------------------------------------------
AVERAGERATING                                                                   
-------------                                                                   
          7.3                                                                   
                                                                                
Fastlane                                                                        
Action,Crime,Thriller                                                           
          7.5                                                                   
                                                                                

SQL> SELECT * FROM
  2  (SELECT B.ORIGINALTITLE, B.GENRES, R.AVERAGERATING
  3  FROM TITLE_BASICS B, TITLE_RATINGS R
  4  WHERE B.ENDYEAR BETWEEN '2001'AND '2005'
  5  AND B.GENRES LIKE '%Adventure%Sci-Fi%'
  6  AND B.TCONST=R.TCONST
  7  ORDER BY R.AVERAGERATING DESC)
  8  WHERE ROWNUM <=5;

ORIGINALTITLE                                                                   
--------------------------------------------------------------------------------
GENRES                                                                          
--------------------------------------------------------------------------------
AVERAGERATING                                                                   
-------------                                                                   
Firefly                                                                         
Adventure,Drama,Sci-Fi                                                          
          9.1                                                                   
                                                                                
Portal                                                                          
Adventure,Fantasy,Sci-Fi                                                        
          8.8                                                                   

ORIGINALTITLE                                                                   
--------------------------------------------------------------------------------
GENRES                                                                          
--------------------------------------------------------------------------------
AVERAGERATING                                                                   
-------------                                                                   
                                                                                
Arga??: La proph??tie                                                           
Adventure,Animation,Sci-Fi                                                      
          8.6                                                                   
                                                                                
Farscape                                                                        
Adventure,Drama,Sci-Fi                                                          

ORIGINALTITLE                                                                   
--------------------------------------------------------------------------------
GENRES                                                                          
--------------------------------------------------------------------------------
AVERAGERATING                                                                   
-------------                                                                   
          8.3                                                                   
                                                                                
Star Trek: Voyager                                                              
Action,Adventure,Sci-Fi                                                         
          7.7                                                                   
                                                                                

SQL> 
SQL> SELECT * FROM
  2  (SELECT B.ORIGINALTITLE, B.GENRES, R.AVERAGERATING
  3  FROM TITLE_BASICS B, TITLE_RATINGS R
  4  WHERE B.ENDYEAR BETWEEN '2001'AND '2005'
  5  AND B.GENRES LIKE '%Adventure%Sci-Fi%'
  6  AND B.TCONST=R.TCONST
  7  ORDER BY R.AVERAGERATING)
  8  WHERE ROWNUM <=5;

ORIGINALTITLE                                                                   
--------------------------------------------------------------------------------
GENRES                                                                          
--------------------------------------------------------------------------------
AVERAGERATING                                                                   
-------------                                                                   
Power Rangers S.P.D.                                                            
Action,Adventure,Sci-Fi                                                         
          6.4                                                                   
                                                                                
Power Rangers DinoThunder                                                       
Action,Adventure,Sci-Fi                                                         
          6.6                                                                   

ORIGINALTITLE                                                                   
--------------------------------------------------------------------------------
GENRES                                                                          
--------------------------------------------------------------------------------
AVERAGERATING                                                                   
-------------                                                                   
                                                                                
2030 CE                                                                         
Adventure,Sci-Fi                                                                
          7.1                                                                   
                                                                                
First Wave                                                                      
Adventure,Drama,Sci-Fi                                                          

ORIGINALTITLE                                                                   
--------------------------------------------------------------------------------
GENRES                                                                          
--------------------------------------------------------------------------------
AVERAGERATING                                                                   
-------------                                                                   
          7.2                                                                   
                                                                                
Immortal Grand Prix                                                             
Adventure,Animation,Sci-Fi                                                      
          7.2                                                                   
                                                                                

SQL> SPOOL OFF;
